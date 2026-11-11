using System.Data.Common;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// What a failed Message Store call says about the store, read from its
/// SQLSTATE. The adapter's log lines name the class, so an alert can tell a
/// read-only store or a lost grant from an outage, and a record whose own data
/// the store cannot hold takes the terminal path instead of being read again.
/// The classes and SQLSTATEs are the Camel adapter's.
/// </summary>
internal enum PlatformStoreErrorClass
{
    /// <summary>Class 22 or 54000: this record's data cannot be stored; a retry cannot help.</summary>
    RecordData,

    /// <summary>25006: the database is read-only, for example a standby after a failover.</summary>
    ReadOnly,

    /// <summary>42501 or class 28: the login lacks a privilege.</summary>
    Privilege,

    /// <summary>Another class 42, class 23, 3D000 or 3F000: the schema or its constraints do not match the adapter.</summary>
    Schema,

    /// <summary>Anything else, including a timeout, a cancellation or a broken connection without a SQLSTATE.</summary>
    Unavailable,
}

internal static class PlatformStoreFailures
{
    private const int MaxCauses = 20;

    /// <summary>
    /// Classifies a failure by the outermost exception in its inner-exception
    /// chain that has a SQLSTATE.
    /// </summary>
    internal static PlatformStoreErrorClass Classify(Exception? failure) => ClassOf(SqlState(failure));

    internal static bool IsRecordData(Exception? failure) => Classify(failure) == PlatformStoreErrorClass.RecordData;

    /// <summary>The SQLSTATE <see cref="Classify"/> reads, or null when the chain has none.</summary>
    internal static string? SqlState(Exception? failure)
    {
        var seen = new HashSet<Exception>(ReferenceEqualityComparer.Instance);
        for (var current = failure; current is not null && seen.Count <= MaxCauses && seen.Add(current); current = current.InnerException)
        {
            if (current is DbException database && !string.IsNullOrWhiteSpace(database.SqlState))
            {
                return database.SqlState;
            }
        }

        return null;
    }

    internal static PlatformStoreErrorClass ClassOf(string? sqlState)
    {
        if (sqlState is null)
        {
            return PlatformStoreErrorClass.Unavailable;
        }

        if (sqlState.StartsWith("22", StringComparison.Ordinal) || sqlState == "54000")
        {
            return PlatformStoreErrorClass.RecordData;
        }

        if (sqlState == "25006")
        {
            return PlatformStoreErrorClass.ReadOnly;
        }

        if (sqlState == "42501" || sqlState.StartsWith("28", StringComparison.Ordinal))
        {
            return PlatformStoreErrorClass.Privilege;
        }

        if (sqlState.StartsWith("42", StringComparison.Ordinal) || sqlState.StartsWith("23", StringComparison.Ordinal)
            || sqlState is "3D000" or "3F000")
        {
            return PlatformStoreErrorClass.Schema;
        }

        return PlatformStoreErrorClass.Unavailable;
    }

    /// <summary>The class as the adapter's log lines name it, such as <c>read-only</c>.</summary>
    internal static string Label(this PlatformStoreErrorClass errorClass) => errorClass switch
    {
        PlatformStoreErrorClass.RecordData => "record-data",
        PlatformStoreErrorClass.ReadOnly => "read-only",
        PlatformStoreErrorClass.Privilege => "privilege",
        PlatformStoreErrorClass.Schema => "schema",
        _ => "unavailable",
    };

    /// <summary>
    /// A failure text as the store can hold it: PostgreSQL text cannot hold a
    /// NUL, so a failure's own message must not make its step or record
    /// unstorable.
    /// </summary>
    internal static string Storable(string text) => text.Replace('\u0000', '�');
}
