using System.Data.Common;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// Confirms that a Message Store write is durable. With synchronous
/// replication, a commit waits for a standby; when that wait is cancelled (the
/// client's command timeout, a cancellation token), PostgreSQL ends it with a
/// WARNING and reports the commit as done, although it exists on the primary
/// only and is lost if the primary fails over. Npgsql returns normally. That
/// warning has only the generic SQLSTATE 01000 and a localized text, so any
/// warning the commit itself reports makes the write count as failed: nothing
/// it gates (an offset commit, a step skip, a replay's produce) may follow, and
/// the idempotent write is tried again.
/// </summary>
internal sealed class PlatformCommitConfirmation : IDisposable
{
    private readonly NpgsqlConnection connection;
    private volatile bool committing;
    private volatile string? warning;

    private PlatformCommitConfirmation(NpgsqlConnection connection)
    {
        this.connection = connection;
        connection.Notice += OnNotice;
    }

    internal static PlatformCommitConfirmation Watch(NpgsqlConnection connection) => new(connection);

    /// <summary>Commits the transaction, and throws when the commit reported a warning.</summary>
    internal async Task CommitAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        // Only what the commit itself reports counts, not a warning an
        // earlier statement of the transaction raised.
        warning = null;
        committing = true;
        try
        {
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            committing = false;
        }

        if (warning is { } sqlState)
        {
            throw new PlatformCommitUnconfirmedException(sqlState);
        }
    }

    public void Dispose() => connection.Notice -= OnNotice;

    private void OnNotice(object sender, NpgsqlNoticeEventArgs args)
    {
        // A warning-level message has a SQLSTATE outside class 00, which
        // notices use.
        if (committing && !args.Notice.SqlState.StartsWith("00", StringComparison.Ordinal))
        {
            warning ??= args.Notice.SqlState;
        }
    }
}

/// <summary>
/// A Message Store write whose commit PostgreSQL reported with a warning, as
/// when a synchronous replication wait was cancelled: a store failure. Its
/// SQLSTATE is 08007 (transaction resolution unknown), whatever the warning's
/// own, so the warning cannot make it count as the record's own data.
/// </summary>
internal sealed class PlatformCommitUnconfirmedException(string warningSqlState)
    : DbException($"The Message Store did not confirm the commit as durable: the commit reported a warning (SQLSTATE {warningSqlState}), as when its wait for synchronous replication ends early; the write is treated as failed.")
{
    internal const string UnconfirmedSqlState = "08007";

    public override string SqlState => UnconfirmedSqlState;
}
