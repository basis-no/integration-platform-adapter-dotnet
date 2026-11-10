using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

public interface IPlatformStepExecutor
{
    Task<PlatformStepExecutionResult> ExecuteAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken);

    /// <summary>
    /// As <c>ExecuteAsync</c>, but the operation receives the step's stable
    /// idempotency key before the external call. A separate name, not an
    /// overload, so no call that compiled against <c>ExecuteAsync</c> becomes
    /// ambiguous. The default body derives the key from the message id and
    /// step id as given and calls <c>ExecuteAsync</c>; an implementation that
    /// changes the step id or the key it records must override this method.
    /// </summary>
    Task<PlatformStepExecutionResult> ExecuteWithContextAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<PlatformStepExecutionContext, CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        // Keep implementations of the original interface source and binary
        // compatible. Their ExecuteAsync still owns execution semantics.
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(stepId);
        ArgumentNullException.ThrowIfNull(operation);
        var context = new PlatformStepExecutionContext(PlatformStepExecutor.IdempotencyKey(message.MessageId, stepId), stepId);
        return ExecuteAsync(message, stepId, endpointUri, targetRef, ct => operation(context, ct), cancellationToken);
    }
}

public sealed record PlatformStepExecutionContext(string IdempotencyKey, string StepId);

public sealed record PlatformStepExecutionResult(
    string IdempotencyKey,
    string StepId,
    string Status,
    bool Skipped);

public sealed class PlatformStepExecutor : IPlatformStepExecutor
{
    private const string Delivered = "DELIVERED";
    private const string Failed = "FAILED";
    private const string SkippedStatus = "SKIPPED";
    private const string RedoStepsHeader = "x-bip-redo-steps";

    // Query parameter names that carry credentials in URIs more often than in
    // headers: ?key= API keys, ?code= function keys, ?sig= shared access
    // signatures and ?pwd= passwords.
    private static readonly HashSet<string> UriCredentialNames = new(StringComparer.Ordinal) { "key", "code", "sig", "pwd" };

    private static readonly string[] UriCredentialNameParts =
    [
        "passphrase", "passcode", "privatekey", "accesskey", "accountkey", "connectionstring",
    ];

    // The option names Camel 4.18 treats as sensitive (SensitiveUtils). The
    // Camel adapter records a step's endpoint as URISupport.sanitizeUri masks
    // it, which masks a query parameter whose name contains any of them.
    private static readonly string[] CamelSensitiveKeys =
    [
        "accesskey", "accesstoken", "accesstokensecret", "accountkey", "accountsid", "acltoken", "api_key",
        "api_secret", "apikey", "apipassword", "apiuser", "apiusername", "authenticationtoken", "authkey",
        "authorizationtoken", "authtoken", "azureclientid", "azureclientsecret", "blobaccesskey",
        "blobstoragesharedkeycredential", "certresourcepassword", "clientid", "clientsecret",
        "clientsecretcredential", "connectionstring", "consumerkey", "consumersecret", "emailaddress",
        "httpproxypassword", "kerberosauthticket", "keypassword", "keystorepassword", "login", "oauthclientid",
        "oauthclientsecret", "oauthtoken", "oauthtokenurl", "p12filename", "passcode", "passphrase", "password",
        "personalaccesstoken", "privatekey", "privatekeyfile", "privatekeyname", "privatekeypassword",
        "proxyauthpassword", "proxyauthusername", "proxypassword", "proxyuser", "proxyusername", "publickeyid",
        "publishkey", "queueownerawsaccountid", "realm", "refreshtoken", "sascredential", "sasljaasconfig",
        "saslpassword", "sassignature", "secret", "secretkey", "securerandom", "serviceinstanceid",
        "sessiontoken", "sharedaccesskey", "sourceblobaccesskey", "sslkeypassword", "sslkeystorepassword",
        "sslpassword", "ssltruststorepassword", "subscribekey", "systemid", "token", "tokencredential",
        "truststorepassword", "user", "userauthenticationcredentials", "username", "userpassword",
        "verificationcode", "webhookverifytoken", "zookeeperpassword",
    ];

    // A bound on the two patterns below: a pathological URI must not stall the
    // step. On timeout nothing of the URI is recorded.
    private static readonly TimeSpan MaskTimeout = TimeSpan.FromSeconds(1);

    // Java's '.' does not match these line terminators; .NET's matches all
    // but '\n'. The user info pattern spells it out to match as Camel does.
    private const string JavaDot = "[^\n\r\u0085\u2028\u2029]";

    // URISupport.sanitizeUri's two patterns, unchanged, so both adapters mask
    // these values the same way: the value of such a parameter up to the next
    // '&', or its whole RAW(...) or RAW{...} form, and the password in the
    // user info, keeping the user name.
    private static readonly Regex CamelSecretParameter = new(
        "([?&][^=]*(?:" + string.Join('|', CamelSensitiveKeys) + ")[^=]*)=(RAW(([{][^}]*[}])|([(][^)]*[)]))|[^&]*)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        MaskTimeout);

    private static readonly Regex CamelUserInfoPassword = new(
        $"({JavaDot}*://{JavaDot}*?:)({JavaDot}*)(@)",
        RegexOptions.None,
        MaskTimeout);

    private readonly NpgsqlDataSource dataSource;
    private readonly PlatformStepObservationCollector observations;
    private readonly ILogger<PlatformStepExecutor> logger;

    public PlatformStepExecutor(
        NpgsqlDataSource dataSource,
        PlatformStepObservationCollector observations,
        ILogger<PlatformStepExecutor> logger)
    {
        this.dataSource = dataSource;
        this.observations = observations;
        this.logger = logger;
    }

    /// <summary>
    /// False with the noop step outcome store: nothing is read from or written
    /// to bip_step_outcome, so every step runs. steps_json still records it.
    /// </summary>
    internal bool UsesStepLedger { get; init; } = true;

    public async Task<PlatformStepExecutionResult> ExecuteAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return await ExecuteWithContextAsync(message, stepId, endpointUri, targetRef, (_, ct) => operation(ct), cancellationToken);
    }

    public async Task<PlatformStepExecutionResult> ExecuteWithContextAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<PlatformStepExecutionContext, CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(stepId);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointUri);
        ArgumentNullException.ThrowIfNull(operation);

        var idempotencyKey = IdempotencyKey(message.MessageId, stepId);
        // The operation calls the real destination; only the recorded copy of
        // the URI is masked.
        var recordedUri = MaskEndpointUri(endpointUri);
        if (UsesStepLedger
            && !ShouldRedo(message, stepId, idempotencyKey, logger)
            && await WasDeliveredAsync(idempotencyKey, cancellationToken))
        {
            await WarnAboutSharedStepKeyAsync(message, stepId, idempotencyKey, cancellationToken);
            Observe(message.MessageId, stepId, SkippedStatus, idempotencyKey, recordedUri, targetRef, null);
            return new PlatformStepExecutionResult(idempotencyKey, stepId, SkippedStatus, true);
        }

        try
        {
            await operation(new PlatformStepExecutionContext(idempotencyKey, stepId), cancellationToken);
            if (UsesStepLedger)
            {
                await RecordAsync(message.MessageId, idempotencyKey, stepId, Delivered, recordedUri, targetRef, null, cancellationToken);
            }

            Observe(message.MessageId, stepId, Delivered, idempotencyKey, recordedUri, targetRef, null);
            return new PlatformStepExecutionResult(idempotencyKey, stepId, Delivered, false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // A cancellation the caller did not request, such as an HttpClient
            // timeout, is a failed step like any other.
            if (UsesStepLedger)
            {
                await RecordAsync(message.MessageId, idempotencyKey, stepId, Failed, recordedUri, targetRef, ex.Message, cancellationToken);
            }

            Observe(message.MessageId, stepId, Failed, idempotencyKey, recordedUri, targetRef, ex.Message);
            throw;
        }
    }

    internal static string IdempotencyKey(string messageId, string stepId) => $"{messageId}:{stepId}";

    /// <summary>
    /// Masks what bip_step_outcome and steps_json would otherwise keep in clear
    /// text. The result is the Camel adapter's value for the URI, so both
    /// adapters record the same value, except that every character this
    /// adapter's own rule masks in the URI is masked too: the whole user info,
    /// and the values of query parameters whose names are credential-like by
    /// the DLQ header rule or one of the URI names above. Both rules read the
    /// URI as given, so one rule's replacement cannot hide a value from the
    /// other.
    /// </summary>
    internal static string MaskEndpointUri(string endpointUri)
    {
        List<(char Character, int Source)> camel;
        try
        {
            camel = CamelSanitizedCharacters(endpointUri);
        }
        catch (RegexMatchTimeoutException)
        {
            return PlatformMessageConsumerService.MaskedHeaderValue;
        }

        var adapterMasked = AdapterMaskedCharacters(endpointUri);
        bool IsMasked(int index) => camel[index].Source < 0 || adapterMasked[camel[index].Source];
        var masked = new StringBuilder(endpointUri.Length);
        for (var index = 0; index < camel.Count;)
        {
            if (!IsMasked(index))
            {
                masked.Append(camel[index++].Character);
                continue;
            }

            // A run of Camel's masks and characters this adapter masks becomes
            // one mask; a run of Camel's masks alone stays as Camel wrote it.
            var run = index;
            var adds = false;
            for (; index < camel.Count && IsMasked(index); index++)
            {
                adds |= camel[index].Source >= 0;
            }

            if (adds)
            {
                masked.Append(PlatformMessageConsumerService.MaskedHeaderValue);
            }
            else
            {
                for (; run < index; run++)
                {
                    masked.Append(camel[run].Character);
                }
            }
        }

        return masked.ToString();
    }

    /// <summary>
    /// Camel's URISupport.sanitizeUri: the parameter pattern replaced
    /// everywhere, then the user info pattern once on that result. Each
    /// character of the result keeps the index it had in the URI, or -1 for a
    /// mask Camel wrote.
    /// </summary>
    private static List<(char Character, int Source)> CamelSanitizedCharacters(string uri)
    {
        var parameters = new List<(char Character, int Source)>(uri.Length);
        var position = 0;
        foreach (Match match in CamelSecretParameter.Matches(uri))
        {
            // "$1=xxxxxx": the name and '=' stay, the value becomes the mask.
            var value = match.Groups[2];
            AppendSource(parameters, uri, position, value.Index);
            AppendMask(parameters);
            position = value.Index + value.Length;
        }

        AppendSource(parameters, uri, position, uri.Length);
        var userInfo = CamelUserInfoPassword.Match(new string(parameters.Select(character => character.Character).ToArray()));
        if (!userInfo.Success)
        {
            return parameters;
        }

        // "$1xxxxxx$3": the password becomes the mask.
        var password = userInfo.Groups[2];
        var sanitized = parameters.GetRange(0, password.Index);
        AppendMask(sanitized);
        sanitized.AddRange(parameters.Skip(password.Index + password.Length));
        return sanitized;
    }

    private static void AppendSource(List<(char Character, int Source)> characters, string uri, int start, int end)
    {
        for (var index = start; index < end; index++)
        {
            characters.Add((uri[index], index));
        }
    }

    private static void AppendMask(List<(char Character, int Source)> characters)
    {
        foreach (var character in PlatformMessageConsumerService.MaskedHeaderValue)
        {
            characters.Add((character, -1));
        }
    }

    /// <summary>
    /// The characters of the URI this adapter's own rule masks: the user info
    /// of the authority, and the value of each query parameter with a
    /// credential-like name.
    /// </summary>
    private static bool[] AdapterMaskedCharacters(string uri)
    {
        var masked = new bool[uri.Length];
        var queryOrFragment = uri.IndexOfAny(['?', '#']);
        var hierarchyEnd = queryOrFragment < 0 ? uri.Length : queryOrFragment;
        // The authority follows "scheme://" or, in a network-path reference
        // such as "//host/path", the leading "//".
        var scheme = uri.StartsWith("//", StringComparison.Ordinal) ? -1 : uri.IndexOf("://", 0, hierarchyEnd, StringComparison.Ordinal);
        if (uri.StartsWith("//", StringComparison.Ordinal) || scheme >= 0)
        {
            var authorityStart = scheme < 0 ? 2 : scheme + 3;
            var slash = uri.IndexOf('/', authorityStart, hierarchyEnd - authorityStart);
            var authorityEnd = slash < 0 ? hierarchyEnd : slash;
            var at = uri.LastIndexOf('@', authorityEnd - 1, authorityEnd - authorityStart);
            Array.Fill(masked, true, authorityStart, Math.Max(0, at - authorityStart));
        }

        if (queryOrFragment < 0 || uri[queryOrFragment] != '?')
        {
            return masked;
        }

        var fragment = uri.IndexOf('#', queryOrFragment);
        var queryEnd = fragment < 0 ? uri.Length : fragment;
        for (var parameterStart = queryOrFragment + 1; parameterStart <= queryEnd;)
        {
            var ampersand = uri.IndexOf('&', parameterStart, queryEnd - parameterStart);
            var parameterEnd = ampersand < 0 ? queryEnd : ampersand;
            var equals = uri.IndexOf('=', parameterStart, parameterEnd - parameterStart);
            if (equals >= 0 && IsCredentialParameter(uri[parameterStart..equals]))
            {
                Array.Fill(masked, true, equals + 1, parameterEnd - equals - 1);
            }

            parameterStart = parameterEnd + 1;
        }

        return masked;
    }

    private static bool IsCredentialParameter(string encodedName)
    {
        var name = Uri.UnescapeDataString(encodedName);
        var lower = name.ToLowerInvariant();
        return PlatformMessageConsumerService.IsCredentialHeader(name)
            || UriCredentialNames.Contains(lower)
            || UriCredentialNameParts.Any(part => lower.Contains(part, StringComparison.Ordinal));
    }

    /// <summary>
    /// The key is <c>messageId:stepId</c> and bip_step_outcome has no
    /// integration column, so a DELIVERED row may not be this call's own
    /// delivery. Log the two ways that happens; the skip itself is unchanged.
    /// </summary>
    private async Task WarnAboutSharedStepKeyAsync(
        PlatformMessage message,
        string stepId,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        switch (observations.ReuseInCurrentInvocation(message.MessageId, idempotencyKey))
        {
            case StepKeyReuse.AfterDeliveryInThisInvocation:
                // This invocation delivered the key, so this call's item is
                // never sent although the message completes: lost data.
                logger.LogError(
                    "Step {StepId} already ran in this handler invocation and is skipped messageId={MessageId}; this call is not sent. "
                    + "The step key is <message-id>:<step-id>; a handler that sends several items through one step needs a distinct step id per item.",
                    stepId,
                    message.MessageId);
                break;
            case StepKeyReuse.Repeated:
                // An earlier attempt delivered the key; which item it sent is
                // unknown, so the reuse stays a warning.
                logger.LogWarning(
                    "Step {StepId} already ran in this handler invocation and is skipped messageId={MessageId}. "
                    + "The step key is <message-id>:<step-id>; a handler that sends several items through one step needs a distinct step id per item.",
                    stepId,
                    message.MessageId);
                break;
        }

        string? journaledBy;
        try
        {
            journaledBy = await JournaledIntegrationAsync(message.MessageId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The lookup only feeds a warning: a failed read must not turn a
            // correct skip into a step failure.
            logger.LogDebug(ex, "Shared step key check skipped messageId={MessageId} step={StepId}", message.MessageId, stepId);
            return;
        }

        if (journaledBy is not null && !string.Equals(journaledBy, message.ApplicationName, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "Step {StepId} is skipped as already delivered, but messageId={MessageId} is journaled by integration {JournaledBy}, not {Integration}. "
                + "Step outcomes are not scoped by integration, so the delivery may be the other integration's. "
                + "Use a new x-bip-message-id per integration, or step ids unique across integrations.",
                stepId,
                message.MessageId,
                journaledBy,
                message.ApplicationName);
        }
    }

    private void Observe(string messageId, string stepId, string status, string idempotencyKey, string endpointUri, string? targetRef, string? error)
    {
        observations.Record(messageId, new PlatformStepObservation(
            stepId,
            status,
            idempotencyKey,
            endpointUri,
            targetRef,
            error,
            DateTimeOffset.UtcNow));
    }

    internal static bool ShouldRedo(PlatformMessage message, string stepId, string idempotencyKey, ILogger logger)
    {
        if (!message.Headers.TryGetValue(RedoStepsHeader, out var redoSteps) || string.IsNullOrWhiteSpace(redoSteps))
        {
            return false;
        }

        try
        {
            var steps = JsonSerializer.Deserialize<List<string>>(redoSteps);
            return steps is not null
                && steps.Any(step => string.Equals(step, stepId, StringComparison.Ordinal)
                    || string.Equals(step, idempotencyKey, StringComparison.Ordinal));
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                ex,
                "Malformed {Header} header value treated as empty (expected a JSON array of step ids) messageId={MessageId} value={Value}",
                RedoStepsHeader,
                message.MessageId,
                redoSteps);
            return false;
        }
    }

    private async Task<bool> WasDeliveredAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select 1
            from bip_step_outcome
            where idempotency_key = @idempotency_key
              and status = 'DELIVERED'
            """,
            connection);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is not null;
    }

    private async Task<string?> JournaledIntegrationAsync(string messageId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select integration
            from bip_message_log
            where message_id = @message_id
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private async Task RecordAsync(
        string messageId,
        string idempotencyKey,
        string stepId,
        string status,
        string endpointUri,
        string? targetRef,
        string? error,
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            insert into bip_step_outcome (
                idempotency_key,
                message_id,
                step_id,
                status,
                endpoint_uri,
                target_ref,
                error,
                occurred_at,
                updated_at
            ) values (
                @idempotency_key,
                @message_id,
                @step_id,
                @status,
                @endpoint_uri,
                @target_ref,
                @error,
                now(),
                now()
            )
            on conflict (idempotency_key) do update
            set message_id = excluded.message_id,
                step_id = excluded.step_id,
                status = excluded.status,
                endpoint_uri = excluded.endpoint_uri,
                target_ref = excluded.target_ref,
                error = excluded.error,
                occurred_at = excluded.occurred_at,
                updated_at = current_timestamp
            where bip_step_outcome.status <> 'DELIVERED'
               or excluded.status = 'DELIVERED'
            """,
            connection);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("step_id", stepId);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("endpoint_uri", endpointUri);
        PlatformMessageStore.AddNullableText(command, "target_ref", targetRef);
        PlatformMessageStore.AddNullableText(command, "error", error);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
