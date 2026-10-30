using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Basis.IntegrationPlatform.MessageStore;

public sealed record PlatformMessage(
    string MessageId,
    string CorrelationId,
    string ApplicationName,
    string EnvironmentName,
    string Tenant,
    string SourceSystem,
    string TargetSystem,
    string ContentType,
    byte[] Payload,
    IReadOnlyDictionary<string, string> Headers,
    int SchemaVersion = 1)
{
    public string PayloadText => Encoding.UTF8.GetString(Payload);
}

public sealed record PlatformMessageResult(bool Succeeded, string? Error = null)
{
    public static PlatformMessageResult Completed() => new(true);
    public static PlatformMessageResult Failed(string error) => new(false, error);
}

public interface IPlatformMessageHandler
{
    Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken);
}

public sealed class MessageReplayRequest
{
    public MessageReplayRequest()
    {
    }

    public MessageReplayRequest(string? requestedBy, string? reason, IReadOnlyList<string>? redoSteps = null)
    {
        RequestedBy = requestedBy;
        Reason = reason;
        RedoSteps = redoSteps ?? [];
    }

    [JsonPropertyName("requestedBy")]
    public string? RequestedBy { get; init; }

    [JsonPropertyName("reason")]
    public string? Reason { get; init; }

    [JsonPropertyName("redoSteps")]
    [JsonConverter(typeof(RedoStepsJsonConverter))]
    public IReadOnlyList<string> RedoSteps { get; init; } = [];
}

public static class MessageReplayErrorCodes
{
    public const string NotFound = "NOT_FOUND";
    public const string NotReplayable = "NOT_REPLAYABLE";
    public const string PublishFailed = "PUBLISH_FAILED";
}

public sealed record MessageReplayResult(
    string Status,
    string MessageId,
    string Integration,
    string TargetTopic,
    int Attempt,
    string? ReplayId = null,
    string? Error = null,
    string? ErrorCode = null);

public sealed class RedoStepsJsonConverter : JsonConverter<IReadOnlyList<string>>
{
    public override IReadOnlyList<string> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType is JsonTokenType.Null)
        {
            return [];
        }

        if (reader.TokenType is JsonTokenType.True or JsonTokenType.False)
        {
            return [];
        }

        if (reader.TokenType is JsonTokenType.String)
        {
            var value = reader.GetString();
            return string.IsNullOrWhiteSpace(value) ? [] : [value];
        }

        if (reader.TokenType is not JsonTokenType.StartArray)
        {
            throw new JsonException("redoSteps must be an array of step ids.");
        }

        var steps = new List<string>();
        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndArray)
            {
                return steps;
            }

            if (reader.TokenType is not JsonTokenType.String)
            {
                throw new JsonException("redoSteps must contain only strings.");
            }

            var step = reader.GetString();
            if (!string.IsNullOrWhiteSpace(step))
            {
                steps.Add(step);
            }
        }

        throw new JsonException("redoSteps array was not closed.");
    }

    public override void Write(Utf8JsonWriter writer, IReadOnlyList<string> value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var step in value)
        {
            writer.WriteStringValue(step);
        }

        writer.WriteEndArray();
    }
}
