using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// Timeouts for every Message Store call, well under the consumer's poll
/// interval (300 s by default), so a hung store, a held lock or a dead
/// connection fails the call as a store failure instead of stalling the
/// consumer until it leaves its group. Each is a default for the adapter's
/// own datasource: a value the connection string sets wins.
/// </summary>
internal static class PlatformStoreTimeouts
{
    /// <summary>Seconds to open a connection or wait for a pooled one (Npgsql's default is 15).</summary>
    internal const int ConnectSeconds = 5;

    /// <summary>Seconds a command may run on the client side (Npgsql's default is 30); above the server's statement timeout, so the server's error arrives first.</summary>
    internal const int CommandSeconds = 20;

    /// <summary>Seconds of inactivity before Npgsql checks a connection with a keepalive (off by default).</summary>
    internal const int KeepaliveSeconds = 10;

    /// <summary>Server-side statement_timeout for the adapter's sessions, in milliseconds.</summary>
    internal const int StatementTimeoutMs = 15_000;

    /// <summary>Server-side lock_timeout for the adapter's sessions, in milliseconds.</summary>
    internal const int LockTimeoutMs = 5_000;

    /// <summary>
    /// The connection string with each timeout the deployment did not set.
    /// The server-side timeouts go in the startup <c>Options</c>, after any
    /// <c>PGOPTIONS</c> Npgsql would otherwise use. A connection string that
    /// sets <c>Options</c> itself, also an empty one, is left as it is, and
    /// <paramref name="serverTimeouts"/> false adds none, for a pooler that
    /// refuses startup parameters.
    /// </summary>
    internal static string Apply(string connectionString, bool serverTimeouts = true)
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

        if (serverTimeouts && !Has("Options"))
        {
            // Npgsql uses PGOPTIONS only when the connection string has no
            // Options; keep what it carries (a search_path, say). The last -c
            // wins, so the defaults go first and its own timeouts win.
            var environment = Environment.GetEnvironmentVariable("PGOPTIONS");
            var timeouts = $"-c statement_timeout={StatementTimeoutMs} -c lock_timeout={LockTimeoutMs}";
            builder.Options = string.IsNullOrWhiteSpace(environment) ? timeouts : $"{timeouts} {environment.Trim()}";
        }

        return builder.ConnectionString;
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
