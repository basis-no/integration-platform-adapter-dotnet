using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// Confirms that a Message Store write is durable. With synchronous
/// replication, a commit waits for a standby; when that wait is cancelled (the
/// client's command timeout, a cancellation token), PostgreSQL ends it with a
/// WARNING and reports the commit as done, although it exists on the primary
/// only and is lost if the primary fails over. Npgsql returns normally. A
/// write that saw that warning is treated as failed: nothing it gates (an
/// offset commit, a step skip, a replay's produce) may follow, and the
/// idempotent write is tried again.
/// </summary>
internal sealed class PlatformCommitConfirmation : IDisposable
{
    // PostgreSQL's generic warning class; the cancelled synchronous
    // replication wait has no more specific code, and its text is localized.
    private const string WarningSqlState = "01000";

    private readonly NpgsqlConnection connection;
    private volatile bool unconfirmed;

    private PlatformCommitConfirmation(NpgsqlConnection connection)
    {
        this.connection = connection;
        connection.Notice += OnNotice;
    }

    internal static PlatformCommitConfirmation Watch(NpgsqlConnection connection) => new(connection);

    /// <summary>Throws when the write's commit was not confirmed.</summary>
    internal void ThrowIfUnconfirmed()
    {
        if (unconfirmed)
        {
            throw new PlatformCommitUnconfirmedException();
        }
    }

    public void Dispose() => connection.Notice -= OnNotice;

    private void OnNotice(object sender, NpgsqlNoticeEventArgs args)
    {
        if (args.Notice.InvariantSeverity == "WARNING" && args.Notice.SqlState == WarningSqlState)
        {
            unconfirmed = true;
        }
    }
}

/// <summary>
/// A Message Store write whose commit PostgreSQL could not confirm as durable,
/// as when a synchronous replication wait was cancelled: a store failure.
/// </summary>
internal sealed class PlatformCommitUnconfirmedException()
    : Exception("The Message Store did not confirm the commit as durable (its wait for synchronous replication ended early); the write is treated as failed.");
