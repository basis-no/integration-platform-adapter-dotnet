using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// Timeouts for every Message Store call, well under the consumer's poll
/// interval (300 s by default), so a hung store, a held lock or a dead
/// connection fails the call as a store failure instead of stalling the
/// consumer until it leaves its group. The client-side ones are defaults for
/// the adapter's own datasource: a value the connection string sets wins. The
/// server-side ones are set in each Message Store transaction, for that
/// transaction alone.
/// </summary>
internal static class PlatformStoreTimeouts
{
    /// <summary>Seconds to open a connection or wait for a pooled one (Npgsql's default is 15).</summary>
    internal const int ConnectSeconds = 5;

    /// <summary>Seconds a command may run on the client side (Npgsql's default is 30); above the server's statement timeout, so the server's error arrives first.</summary>
    internal const int CommandSeconds = 20;

    /// <summary>Seconds of inactivity before Npgsql checks a connection with a keepalive (off by default).</summary>
    internal const int KeepaliveSeconds = 10;

    /// <summary>Server-side statement_timeout for each Message Store transaction, in milliseconds.</summary>
    internal const int StatementTimeoutMs = 15_000;

    /// <summary>Server-side lock_timeout for each Message Store transaction, in milliseconds.</summary>
    internal const int LockTimeoutMs = 5_000;

    // client_min_messages at warning: a role, database or connection setting
    // of error would hide the warning of an unconfirmed synchronous commit,
    // which would then count as confirmed (PlatformCommitConfirmation). The
    // commit sends that warning before the transaction's settings end.
    private const string ServerSettingsSql =
        "select set_config('statement_timeout', @statement_timeout, true), " +
        "set_config('lock_timeout', @lock_timeout, true), " +
        "set_config('client_min_messages', 'warning', true)";

    /// <summary>
    /// The connection string with each client-side timeout the deployment did
    /// not set. It adds no startup <c>Options</c>: a pooler such as PgBouncer
    /// drops or refuses that parameter, and a session setting would stay on a
    /// pooled server connection. The connection string's own <c>Options</c>,
    /// or the <c>PGOPTIONS</c> Npgsql uses without it, is left as it is.
    /// </summary>
    internal static string Apply(string connectionString)
    {
        var given = Keywords(connectionString);
        bool Has(params string[] keys) => keys.Any(key => given.Contains(Normalize(key)));
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (!Has("Timeout"))
        {
            builder.Timeout = ConnectSeconds;
        }

        if (!Has("Command Timeout", "CommandTimeout"))
        {
            builder.CommandTimeout = CommandSeconds;
        }

        if (!Has("Keepalive"))
        {
            builder.KeepAlive = KeepaliveSeconds;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Begins a Message Store transaction. With <paramref name="serverTimeouts"/>
    /// it first sets statement_timeout, lock_timeout and client_min_messages
    /// for this transaction alone, as the Camel adapter does: a statement
    /// reaches the server also through a pooler that drops or refuses startup
    /// options, and the connection goes back to the pool without them. The
    /// adapter runs every store statement in such a transaction, a single read
    /// or write too, so each one gets them.
    /// </summary>
    internal static async Task<NpgsqlTransaction> BeginTransactionAsync(
        NpgsqlConnection connection, bool serverTimeouts, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        if (!serverTimeouts)
        {
            return transaction;
        }

        try
        {
            await using var command = new NpgsqlCommand(ServerSettingsSql, connection, transaction);
            command.Parameters.AddWithValue("statement_timeout", $"{StatementTimeoutMs}ms");
            command.Parameters.AddWithValue("lock_timeout", $"{LockTimeoutMs}ms");
            await command.ExecuteNonQueryAsync(cancellationToken);
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// The keywords a connection string sets, also those with an empty value,
    /// read outside quoted values so a password can't look like a keyword.
    /// </summary>
    internal static HashSet<string> Keywords(string connectionString)
    {
        var keywords = new HashSet<string>(StringComparer.Ordinal);
        var index = 0;
        while (index < connectionString.Length)
        {
            var equals = connectionString.IndexOf('=', index);
            if (equals < 0)
            {
                break;
            }

            // Empty segments (";;", "; ;") before a keyword are no part of it.
            var key = Normalize(connectionString[index..equals].TrimStart(';', ' ', '\t'));
            if (key.Length > 0)
            {
                keywords.Add(key);
            }

            index = equals + 1;
            while (index < connectionString.Length && char.IsWhiteSpace(connectionString[index]))
            {
                index++;
            }

            if (index < connectionString.Length && connectionString[index] is '\'' or '"')
            {
                // A quoted value ends at its closing quote; a doubled quote is
                // an escaped one.
                var quote = connectionString[index++];
                while (index < connectionString.Length)
                {
                    if (connectionString[index] == quote)
                    {
                        if (index + 1 < connectionString.Length && connectionString[index + 1] == quote)
                        {
                            index += 2;
                            continue;
                        }

                        index++;
                        break;
                    }

                    index++;
                }
            }

            var semicolon = connectionString.IndexOf(';', index);
            index = semicolon < 0 ? connectionString.Length : semicolon + 1;
        }

        return keywords;
    }

    private static string Normalize(string key) => string.Join(' ', key.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)).ToLowerInvariant();
}
