using System.Data.Common;
using System.Text.RegularExpressions;
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
    /// The server-side timeouts go in the startup <c>Options</c>; a
    /// connection string that sets <c>Options</c> itself is left as it is,
    /// for a pooler that refuses startup parameters.
    /// </summary>
    internal static string Apply(string connectionString)
    {
        var given = new DbConnectionStringBuilder { ConnectionString = connectionString };
        bool Has(params string[] keys) => keys.Any(given.ContainsKey);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (!Has("Timeout"))
        {
            builder.Timeout = ConnectSeconds;
        }

        if (!Has("Command Timeout", "CommandTimeout"))
        {
            builder.CommandTimeout = CommandSeconds;
        }

        if (!Has("Keepalive", "Keep Alive", "KeepAlive"))
        {
            builder.KeepAlive = KeepaliveSeconds;
        }

        // An empty Options= is a choice too (no startup parameters at all),
        // and the generic builder drops empty values, so look at the text.
        if (!Has("Options") && !Regex.IsMatch(connectionString, @"(^|;)\s*options\s*=", RegexOptions.IgnoreCase))
        {
            builder.Options = $"-c statement_timeout={StatementTimeoutMs} -c lock_timeout={LockTimeoutMs}";
        }

        return builder.ConnectionString;
    }
}
