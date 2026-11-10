using System.Text.RegularExpressions;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// The Message Store tables and columns the adapter needs, read from the store
/// schema it ships (tests/fixtures/postgres/002_adapter_v1_message_store.sql,
/// embedded in the package), so the startup check cannot drift from it.
/// </summary>
internal static partial class PlatformMessageStoreSchema
{
    internal const string ResourceName = "Basis.IntegrationPlatform.MessageStore.Schema.002_adapter_v1_message_store.sql";

    internal const string MessageLogTable = "bip_message_log";
    internal const string MessageAttemptTable = "bip_message_attempt";
    internal const string ReplayAuditTable = "bip_message_replay_audit";
    internal const string StepOutcomeTable = "bip_step_outcome";

    private static readonly HashSet<string> TableConstraints = new(StringComparer.OrdinalIgnoreCase)
    {
        "primary", "constraint", "unique", "check", "foreign", "exclude",
    };

    private static readonly Lazy<IReadOnlyDictionary<string, ShippedTable>> Shipped = new(() => Parse(ReadResource()));

    /// <summary>Every table of the shipped schema, by name.</summary>
    internal static IReadOnlyDictionary<string, ShippedTable> Tables => Shipped.Value;

    /// <summary>
    /// The tables the adapter uses in this configuration: the message log and
    /// attempt tables always, the step outcome table with the Postgres step
    /// outcome store, and the replay audit table when the resend endpoint is
    /// mapped.
    /// </summary>
    internal static IReadOnlyList<ShippedTable> Required(bool stepLedger, bool replay)
    {
        var names = new List<string> { MessageLogTable, MessageAttemptTable };
        if (replay)
        {
            names.Add(ReplayAuditTable);
        }

        if (stepLedger)
        {
            names.Add(StepOutcomeTable);
        }

        return names.Select(name => Tables.TryGetValue(name, out var table)
                ? table
                : throw new InvalidOperationException($"The adapter's shipped store schema does not define table {name}."))
            .ToList();
    }

    /// <summary>
    /// The statement that adds a missing column. A column that must have a
    /// value but has no default can't be added to a table with rows in one
    /// step, so it is added without the requirement, to be filled in before
    /// the requirement is set.
    /// </summary>
    internal static string AddColumnStatement(string table, string definition)
    {
        var column = definition.Split(' ', 2)[0];
        if (PrimaryKey().IsMatch(definition))
        {
            return $"alter table {table} add column if not exists {PrimaryKey().Replace(definition, string.Empty, 1)}; " +
                   $"then give existing rows unique values and run: alter table {table} add primary key ({column})";
        }

        if (!NotNull().IsMatch(definition) || Default().IsMatch(definition))
        {
            return $"alter table {table} add column if not exists {definition}";
        }

        return $"alter table {table} add column if not exists {NotNull().Replace(definition, string.Empty, 1)}; " +
               $"then give existing rows a value and run: alter table {table} alter column {column} set not null";
    }

    /// <summary>Column name to its definition, per table, from the create table statements.</summary>
    internal static IReadOnlyDictionary<string, ShippedTable> Parse(string sql)
    {
        var text = LineComment().Replace(sql, string.Empty);
        var tables = new Dictionary<string, ShippedTable>(StringComparer.Ordinal);
        foreach (Match create in CreateTable().Matches(text))
        {
            var start = create.Index + create.Length;
            var end = start;
            for (var depth = 1; depth > 0 && end < text.Length; end++)
            {
                depth += text[end] switch { '(' => 1, ')' => -1, _ => 0 };
            }

            var body = text[start..(end - 1)];
            var columns = new List<KeyValuePair<string, string>>();
            foreach (var part in TopLevelParts(body))
            {
                var definition = Whitespace().Replace(part.Trim(), " ");
                if (definition.Length == 0)
                {
                    continue;
                }

                var name = definition.Split(' ', 2)[0].ToLowerInvariant();
                if (!TableConstraints.Contains(name))
                {
                    columns.Add(new(name, definition));
                }
            }

            var tableName = create.Groups[1].Value.ToLowerInvariant();
            tables[tableName] = new ShippedTable(tableName, columns, Whitespace().Replace(text[create.Index..end].Trim(), " "));
        }

        return tables;
    }

    private static IEnumerable<string> TopLevelParts(string body)
    {
        var depth = 0;
        var from = 0;
        for (var i = 0; i < body.Length; i++)
        {
            switch (body[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',' when depth == 0:
                    yield return body[from..i];
                    from = i + 1;
                    break;
            }
        }

        yield return body[from..];
    }

    private static string ReadResource()
    {
        using var stream = typeof(PlatformMessageStoreSchema).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"The adapter package lacks its store schema resource {ResourceName}.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [GeneratedRegex(@"create\s+table\s+(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_]*)\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex CreateTable();

    [GeneratedRegex(@"--[^\n]*")]
    private static partial Regex LineComment();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    [GeneratedRegex(@" primary key", RegexOptions.IgnoreCase)]
    private static partial Regex PrimaryKey();

    [GeneratedRegex(@" not null", RegexOptions.IgnoreCase)]
    private static partial Regex NotNull();

    [GeneratedRegex(@" default ", RegexOptions.IgnoreCase)]
    private static partial Regex Default();
}

/// <summary>A table of the shipped store schema: its columns in order, with their definitions, and its create statement.</summary>
internal sealed record ShippedTable(string Name, IReadOnlyList<KeyValuePair<string, string>> Columns, string CreateStatement);
