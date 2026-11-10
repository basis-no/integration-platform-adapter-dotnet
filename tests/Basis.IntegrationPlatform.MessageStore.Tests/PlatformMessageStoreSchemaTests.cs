using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformMessageStoreSchemaTests
{
    [TestMethod]
    public void ShippedSchemaIsTheFixtureSchema()
    {
        using var stream = typeof(PlatformMessageStore).Assembly.GetManifestResourceStream(PlatformMessageStoreSchema.ResourceName);
        Assert.IsNotNull(stream, "The package embeds the store schema.");
        using var reader = new StreamReader(stream);

        Assert.AreEqual(
            File.ReadAllText(Path.Combine(RepositoryRoot(), "tests", "fixtures", "postgres", "002_adapter_v1_message_store.sql")),
            reader.ReadToEnd());
    }

    [TestMethod]
    public void ShippedSchemaHasEveryColumnTheAdapterInserts()
    {
        // Drift check: every column named by an insert statement in the
        // adapter's source is a column of that table in the shipped schema,
        // so the startup check requires everything the adapter writes.
        var inserts = Directory.GetFiles(Path.Combine(RepositoryRoot(), "src", "Basis.IntegrationPlatform.MessageStore"), "*.cs")
            .SelectMany(file => Regex.Matches(File.ReadAllText(file), @"insert into (bip_[a-z_]+) \(([^)]*)\)"))
            .ToList();
        var tables = inserts.Select(insert => insert.Groups[1].Value).Distinct().Order().ToList();

        CollectionAssert.AreEqual(
            new[]
            {
                PlatformMessageStoreSchema.MessageAttemptTable,
                PlatformMessageStoreSchema.MessageLogTable,
                PlatformMessageStoreSchema.ReplayAuditTable,
                PlatformMessageStoreSchema.StepOutcomeTable,
            },
            tables);
        foreach (var insert in inserts)
        {
            var table = insert.Groups[1].Value;
            var shipped = PlatformMessageStoreSchema.Tables[table].Columns.Select(column => column.Key).ToHashSet();
            foreach (var column in insert.Groups[2].Value.Split(',').Select(column => column.Trim()))
            {
                Assert.IsTrue(shipped.Contains(column), $"{table}.{column} is written but not in the shipped schema.");
            }
        }
    }

    [TestMethod]
    public void ParsesColumnsAndCreateStatementsFromTheShippedSchema()
    {
        var log = PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.MessageLogTable];
        var columns = log.Columns.ToDictionary(column => column.Key, column => column.Value);

        Assert.AreEqual("message_id", log.Columns[0].Key);
        Assert.AreEqual("updated_at", log.Columns[^1].Key);
        Assert.AreEqual("payload_deleted_at timestamptz", columns["payload_deleted_at"]);
        Assert.AreEqual(
            "payload_storage text not null default 'metadata_only' check (payload_storage in ('metadata_only', 'postgres_inline'))",
            columns["payload_storage"]);
        Assert.IsFalse(PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.MessageAttemptTable].Columns.Any(column => column.Key == "primary"));
        StringAssert.StartsWith(log.CreateStatement, "create table if not exists bip_message_log ( message_id text primary key,");
        StringAssert.EndsWith(log.CreateStatement, "updated_at timestamptz not null default current_timestamp )");
    }

    [TestMethod]
    public void RequiredTablesFollowTheStepOutcomeStoreAndTheResendEndpoint()
    {
        string[] Names(bool stepLedger, bool replay) =>
            PlatformMessageStoreSchema.Required(stepLedger, replay).Select(table => table.Name).ToArray();

        CollectionAssert.AreEqual(new[] { "bip_message_log", "bip_message_attempt", "bip_step_outcome" }, Names(true, false));
        CollectionAssert.AreEqual(new[] { "bip_message_log", "bip_message_attempt" }, Names(false, false));
        CollectionAssert.AreEqual(
            new[] { "bip_message_log", "bip_message_attempt", "bip_message_replay_audit", "bip_step_outcome" },
            Names(true, true));
    }

    [TestMethod]
    public void AddColumnStatementDefersARequirementExistingRowsCannotMeet()
    {
        Assert.AreEqual(
            ("alter table bip_message_log add column if not exists payload_deleted_at timestamptz", (string?)null),
            PlatformMessageStoreSchema.AddColumnStatement("bip_message_log", "payload_deleted_at timestamptz"));
        Assert.AreEqual(
            ("alter table bip_message_log add column if not exists replayable boolean not null default false", (string?)null),
            PlatformMessageStoreSchema.AddColumnStatement("bip_message_log", "replayable boolean not null default false"));
        Assert.AreEqual(
            ("alter table bip_message_log add column if not exists last_attempt integer",
                (string?)"alter table bip_message_log alter column last_attempt set not null"),
            PlatformMessageStoreSchema.AddColumnStatement("bip_message_log", "last_attempt integer not null"));
        Assert.AreEqual(
            ("alter table bip_message_log add column if not exists message_id text",
                (string?)"alter table bip_message_log add primary key (message_id)"),
            PlatformMessageStoreSchema.AddColumnStatement("bip_message_log", "message_id text primary key"));
    }

    [TestMethod]
    public void EachShippedTableKeepsItsIndexes()
    {
        CollectionAssert.AreEqual(
            new[]
            {
                "create index if not exists bip_message_attempt_message_id_idx on bip_message_attempt (message_id)",
                "create index if not exists bip_message_attempt_integration_status_idx on bip_message_attempt (integration, status)",
            },
            PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.MessageAttemptTable].IndexStatements.ToArray());
        Assert.AreEqual(4, PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.MessageLogTable].IndexStatements.Count);
        Assert.AreEqual(2, PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.ReplayAuditTable].IndexStatements.Count);
        Assert.AreEqual(2, PlatformMessageStoreSchema.Tables[PlatformMessageStoreSchema.StepOutcomeTable].IndexStatements.Count);
    }

    [TestMethod]
    public void MappingTheResendEndpointMakesTheReplayTablesRequired()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddBasisMessageStoreAdapter<SchemaTestHandler>();
        using var app = builder.Build();
        var replayEndpoints = app.Services.GetRequiredService<PlatformReplayEndpoints>();
        Assert.IsFalse(replayEndpoints.Mapped);

        app.MapBasisMessageStoreAdminEndpoints();

        Assert.IsTrue(replayEndpoints.Mapped);
    }

    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (Directory.Exists(Path.Combine(directory.FullName, "tests", "fixtures", "postgres")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("Repository root not found above the test output directory.");
    }

    private sealed class SchemaTestHandler : IPlatformMessageHandler
    {
        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken) =>
            Task.FromResult(PlatformMessageResult.Completed());
    }
}
