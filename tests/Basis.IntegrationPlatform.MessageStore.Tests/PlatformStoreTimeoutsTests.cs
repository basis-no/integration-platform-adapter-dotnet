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
        Assert.AreEqual("-c statement_timeout=15000 -c lock_timeout=5000", applied.Options);
        Assert.AreEqual("synthetic", applied.Password);
        Assert.IsTrue(PlatformStoreTimeouts.CommandSeconds * 1000 > PlatformStoreTimeouts.StatementTimeoutMs);
    }

    [TestMethod]
    [DataRow("Timeout=30;Command Timeout=60;Keepalive=0;Options=", null)]
    [DataRow("timeout=30;CommandTimeout=60;keepalive=0;options=-c search_path=app", "-c search_path=app")]
    public void KeepsEveryTimeoutTheConnectionStringSets(string explicitSettings, string? options)
    {
        var applied = new NpgsqlConnectionStringBuilder(PlatformStoreTimeouts.Apply("Host=postgres;Username=app;" + explicitSettings));

        Assert.AreEqual(30, applied.Timeout);
        Assert.AreEqual(60, applied.CommandTimeout);
        Assert.AreEqual(0, applied.KeepAlive);
        Assert.AreEqual(options, string.IsNullOrEmpty(applied.Options) ? null : applied.Options,
            "An explicit Options, even an empty one, is left as it is, for a pooler that refuses startup parameters.");
    }
}
