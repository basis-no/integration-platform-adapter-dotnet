using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// Tells a Message Store failure that says the store is unavailable (it can't
/// be reached, doesn't answer in time, is shutting down or out of resources,
/// or couldn't confirm a commit as durable) from one that refuses this call's
/// data or was cancelled by the caller's own token. Only an unavailable store
/// ends an attempt without parking; the others would fail the same way on
/// every delivery.
/// </summary>
internal static class PlatformStoreFailures
{
    internal static bool IsOutage(Exception failure) => failure switch
    {
        PlatformCommitUnconfirmedException => true,
        PostgresException postgres => postgres.SqlState is PostgresErrorCodes.QueryCanceled or PostgresErrorCodes.LockNotAvailable
            || postgres.SqlState.StartsWith("08", StringComparison.Ordinal)
            || postgres.SqlState.StartsWith("53", StringComparison.Ordinal)
            || postgres.SqlState.StartsWith("57", StringComparison.Ordinal)
            || postgres.SqlState.StartsWith("58", StringComparison.Ordinal),
        NpgsqlException => true,
        TimeoutException => true,
        _ => false,
    };
}
