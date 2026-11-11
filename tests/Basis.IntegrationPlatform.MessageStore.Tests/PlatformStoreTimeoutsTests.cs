using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformStoreTimeoutsTests
{
    [TestMethod]
    public void AppliesEveryTimeoutTheConnectionStringLeavesOut()
    {
        var applied = new NpgsqlConnectionStringBuilder(
            PlatformStoreTimeouts.Apply("Host=postgres;Database=integration;Username=app;Password=synthetic"));

        Assert.AreEqual(PlatformStoreTimeouts.ConnectSeconds, applied.Timeout);
        Assert.AreEqual(PlatformStoreTimeouts.CommandSeconds, applied.CommandTimeout);
        Assert.AreEqual(PlatformStoreTimeouts.KeepaliveSeconds, applied.KeepAlive);
        Assert.IsTrue(string.IsNullOrEmpty(applied.Options), "The server-side timeouts are set in each transaction, not at startup.");
        Assert.AreEqual("synthetic", applied.Password);
        Assert.IsTrue(PlatformStoreTimeouts.CommandSeconds * 1000 > PlatformStoreTimeouts.StatementTimeoutMs);
    }

    [TestMethod]
    [DataRow("Timeout=30;Command Timeout=60;Keepalive=0;Options=", null)]
    [DataRow("Timeout=30;Command Timeout=60;Keepalive=0;Options=''", null)]
    [DataRow(" TIMEOUT =30; command timeout=60; KEEPALIVE=0; Options = ", null)]
    [DataRow("timeout=30;CommandTimeout=60;keepalive=0;options=-c search_path=app", "-c search_path=app")]
    [DataRow("Timeout=30;Command Timeout=60;Keepalive=0;Options=-c statement_timeout=60000", "-c statement_timeout=60000")]
    [DataRow(";;Timeout=30; ;Command Timeout=60;Keepalive=0;;Options=", null)]
    public void KeepsEveryTimeoutTheConnectionStringSets(string explicitSettings, string? options)
    {
        var applied = new NpgsqlConnectionStringBuilder(PlatformStoreTimeouts.Apply("Host=postgres;Username=app;" + explicitSettings));

        Assert.AreEqual(30, applied.Timeout);
        Assert.AreEqual(60, applied.CommandTimeout);
        Assert.AreEqual(0, applied.KeepAlive);
        Assert.AreEqual(options, string.IsNullOrEmpty(applied.Options) ? null : applied.Options, "The connection string's own Options is left as it is.");
    }

    [TestMethod]
    [DataRow("Password='a;timeout=1'")]
    [DataRow("Application Name=\"x; Timeout=1\"")]
    public void AKeywordInsideAQuotedValueDoesNotCount(string quoted)
    {
        var applied = new NpgsqlConnectionStringBuilder(PlatformStoreTimeouts.Apply("Host=postgres;Username=app;" + quoted));

        Assert.AreEqual(PlatformStoreTimeouts.ConnectSeconds, applied.Timeout);
    }

    [TestMethod]
    public void PgOptionsIsLeftToNpgsql()
    {
        var pgOptions = Environment.GetEnvironmentVariable("PGOPTIONS");
        try
        {
            Environment.SetEnvironmentVariable("PGOPTIONS", "-c search_path=app");
            var applied = new NpgsqlConnectionStringBuilder(PlatformStoreTimeouts.Apply("Host=postgres;Username=app"));
            Assert.IsTrue(string.IsNullOrEmpty(applied.Options), "Without Options in the connection string, Npgsql sends PGOPTIONS itself.");
            Assert.AreEqual(PlatformStoreTimeouts.CommandSeconds, applied.CommandTimeout);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PGOPTIONS", pgOptions);
        }
    }

    [TestMethod]
    [DataRow("on", true)]
    [DataRow(" OFF ", false)]
    [DataRow("", true)]
    public void TheServerTimeoutSwitchReadsOnOrOff(string value, bool expected)
    {
        Assert.AreEqual(expected, new PlatformMessageStoreOptions { PostgresServerTimeouts = value }.UsesPostgresServerTimeouts);
    }

    [TestMethod]
    public void AnUnknownServerTimeoutSwitchFailsValidationNamingTheVariable()
    {
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-orders", KafkaBootstrapServers = "kafka:9092",
            PostgresConnectionString = "Host=postgres;Username=app", PostgresServerTimeouts = "maybe",
        };

        var error = Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
        StringAssert.Contains(error.Message, "BASIS_MESSAGE_STORE_POSTGRES_SERVER_TIMEOUTS");
    }

    [TestMethod]
    public void TheRegistrationAppliesTheTimeoutsToTheAdapterDatasourceOnly()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBasisMessageStoreAdapter<TimeoutTestHandler>(options =>
        {
            options.ApplicationName = "integration-orders";
            options.KafkaBootstrapServers = "kafka:9092";
            options.PostgresConnectionString = "Host=postgres;Username=app;Password=synthetic";
        });
        using var provider = services.BuildServiceProvider();

        var adapter = new NpgsqlConnectionStringBuilder(
            provider.GetRequiredKeyedService<NpgsqlDataSource>(PlatformMessageStoreExtensions.MessageStoreDataSourceKey).ConnectionString);
        var application = new NpgsqlConnectionStringBuilder(provider.GetRequiredService<NpgsqlDataSource>().ConnectionString);

        Assert.AreEqual(PlatformStoreTimeouts.ConnectSeconds, adapter.Timeout);
        Assert.AreEqual(PlatformStoreTimeouts.CommandSeconds, adapter.CommandTimeout);
        Assert.AreEqual(PlatformStoreTimeouts.KeepaliveSeconds, adapter.KeepAlive);
        Assert.IsTrue(string.IsNullOrEmpty(adapter.Options), "Nothing is added to the startup parameters.");
        Assert.AreEqual(15, application.Timeout, "The application's default datasource keeps Npgsql's defaults.");
        Assert.AreEqual(30, application.CommandTimeout);
        Assert.IsTrue(string.IsNullOrEmpty(application.Options));
    }

    private sealed class TimeoutTestHandler : IPlatformMessageHandler
    {
        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken) =>
            Task.FromResult(PlatformMessageResult.Completed());
    }
}
