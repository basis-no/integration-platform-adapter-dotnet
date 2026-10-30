using Microsoft.VisualStudio.TestTools.UnitTesting;
using Confluent.Kafka;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformMessageStoreOptionsTests
{
    [TestMethod]
    public void ValidateDerivesInputTopicFromApplicationName()
    {
        var options = ValidOptions();
        options.InputTopic = "";

        options.Validate();

        Assert.AreEqual("bip.integration-orders.in", options.InputTopic);
    }

    [TestMethod]
    public void DefaultsUseBipNamingAndPayloadPolicy()
    {
        // Relies on the BASIS_MESSAGE_STORE_*/APP_KAFKA_* env vars not being set
        // in the test environment, so the constructor defaults apply.
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-orders",
            KafkaBootstrapServers = "kafka:9092",
            PostgresConnectionString = "Host=postgres;Database=integration;Username=app;Password=secret",
        };

        options.Validate();

        Assert.AreEqual("bip.message.dlq", options.DlqTopic);
        Assert.AreEqual("bip.%s.in", options.ReplayInputTopicPattern);
        Assert.AreEqual(PlatformMessageStoreOptions.PayloadModePostgresInline, options.PayloadMode);
        Assert.AreEqual(1_048_576, options.PayloadMaxBytes);
        Assert.AreEqual(30, options.PayloadRetentionDays);
    }

    [TestMethod]
    public void ValidateNormalizesPayloadModeSpelling()
    {
        var options = ValidOptions();
        options.PayloadMode = "Metadata-Only";

        options.Validate();

        Assert.AreEqual(PlatformMessageStoreOptions.PayloadModeMetadataOnly, options.PayloadMode);
    }

    [TestMethod]
    public void ValidateRejectsUnknownPayloadMode()
    {
        var options = ValidOptions();
        options.PayloadMode = "s3_offload";

        Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
    }

    [TestMethod]
    public void ValidateRejectsNegativePayloadLimits()
    {
        var options = ValidOptions();
        options.PayloadMaxBytes = -1;
        Assert.ThrowsExactly<InvalidOperationException>(options.Validate);

        options = ValidOptions();
        options.PayloadRetentionDays = -1;
        Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
    }

    [TestMethod]
    public void PayloadPolicyReadsEnvironment()
    {
        var mode = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MODE");
        var maxBytes = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES");
        var retention = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS");
        try
        {
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MODE", "metadata_only");
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES", "2048");
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS", "7");

            var options = new PlatformMessageStoreOptions();

            Assert.AreEqual("metadata_only", options.PayloadMode);
            Assert.AreEqual(2048, options.PayloadMaxBytes);
            Assert.AreEqual(7, options.PayloadRetentionDays);
        }
        finally
        {
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MODE", mode);
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES", maxBytes);
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS", retention);
        }
    }

    [TestMethod]
    public void ValidateRejectsMissingKafkaBootstrapServers()
    {
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-orders",
            KafkaBootstrapServers = "",
            PostgresConnectionString = "Host=postgres;Database=integration;Username=app;Password=secret",
        };

        Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
    }

    [TestMethod]
    public void ResolvePostgresConnectionStringAcceptsJdbcEnvironment()
    {
        var direct = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");
        var jdbc = Environment.GetEnvironmentVariable("POSTGRES_JDBC_URL");
        var username = Environment.GetEnvironmentVariable("POSTGRES_USERNAME");
        var password = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        try
        {
            Environment.SetEnvironmentVariable("POSTGRES_CONNECTION_STRING", null);
            Environment.SetEnvironmentVariable("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres.local:5433/integration");
            Environment.SetEnvironmentVariable("POSTGRES_USERNAME", "app");
            Environment.SetEnvironmentVariable("POSTGRES_PASSWORD", "secret");

            var options = new PlatformMessageStoreOptions();

            StringAssert.Contains(options.PostgresConnectionString, "Host=postgres.local");
            StringAssert.Contains(options.PostgresConnectionString, "Port=5433");
            StringAssert.Contains(options.PostgresConnectionString, "Database=integration");
            StringAssert.Contains(options.PostgresConnectionString, "Username=app");
        }
        finally
        {
            Environment.SetEnvironmentVariable("POSTGRES_CONNECTION_STRING", direct);
            Environment.SetEnvironmentVariable("POSTGRES_JDBC_URL", jdbc);
            Environment.SetEnvironmentVariable("POSTGRES_USERNAME", username);
            Environment.SetEnvironmentVariable("POSTGRES_PASSWORD", password);
        }
    }

    [TestMethod]
    public void KafkaConfigurationRequiresAuthenticatedTlsCredentials()
    {
        var options = ValidOptions();
        options.KafkaSecurityProtocol = SecurityProtocol.SaslSsl;
        Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
        options.KafkaSaslUsername = "synthetic-runtime";
        options.KafkaSaslPassword = "synthetic-password";
        options.KafkaSslCaLocation = "/certificates/ca.crt";
        var config = options.CreateKafkaClientConfig();
        Assert.AreEqual(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.AreEqual(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.AreEqual(options.KafkaSaslUsername, config.SaslUsername);
        Assert.AreEqual(options.KafkaSaslPassword, config.SaslPassword);
        Assert.AreEqual(options.KafkaSslCaLocation, config.SslCaLocation);
        Assert.AreEqual(true, config.EnableSslCertificateVerification);
        Assert.AreEqual(SslEndpointIdentificationAlgorithm.Https, config.SslEndpointIdentificationAlgorithm);
    }

    [TestMethod]
    public void LegacyKafkaConfigurationRemainsPlaintextWithoutCredentials()
    {
        var config = ValidOptions().CreateKafkaClientConfig();
        Assert.AreEqual(SecurityProtocol.Plaintext, config.SecurityProtocol);
        Assert.IsNull(config.SaslUsername);
        Assert.IsNull(config.SaslPassword);
    }

    [TestMethod]
    public void SecurityAndConnectionBudgetsReadOperatorEnvironment()
    {
        var settings = new Dictionary<string, string?>
        {
            ["BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL",
            ["BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM"] = "SCRAM-SHA-512",
            ["BASIS_MESSAGE_STORE_KAFKA_SASL_USERNAME"] = "synthetic",
            ["BASIS_MESSAGE_STORE_KAFKA_SASL_PASSWORD"] = "synthetic-password",
            ["POSTGRES_CONNECTION_STRING"] = null,
            ["POSTGRES_JDBC_URL"] = "jdbc:postgresql://postgres.local/integration",
            ["BASIS_MESSAGE_STORE_POSTGRES_MAX_POOL_SIZE"] = "4",
            ["BASIS_MESSAGE_STORE_POSTGRES_SSL_MODE"] = "VerifyFull",
            ["BASIS_MESSAGE_STORE_POSTGRES_ROOT_CERTIFICATE"] = "/certificates/postgres-ca.crt",
        };
        var old = settings.Keys.ToDictionary(key => key, Environment.GetEnvironmentVariable);
        try
        {
            foreach (var entry in settings) Environment.SetEnvironmentVariable(entry.Key, entry.Value);
            var options = new PlatformMessageStoreOptions();
            Assert.AreEqual(SecurityProtocol.SaslSsl, options.KafkaSecurityProtocol);
            Assert.AreEqual(SaslMechanism.ScramSha512, options.KafkaSaslMechanism);
            var postgres = new Npgsql.NpgsqlConnectionStringBuilder(options.PostgresConnectionString);
            Assert.AreEqual(4, postgres.MaxPoolSize);
            Assert.AreEqual(Npgsql.SslMode.VerifyFull, postgres.SslMode);
            Assert.AreEqual("/certificates/postgres-ca.crt", postgres.RootCertificate);
            Environment.SetEnvironmentVariable("BASIS_MESSAGE_STORE_POSTGRES_MAX_POOL_SIZE", "0");
            Assert.ThrowsExactly<InvalidOperationException>(() => new PlatformMessageStoreOptions());
        }
        finally
        {
            foreach (var entry in old) Environment.SetEnvironmentVariable(entry.Key, entry.Value);
        }
    }

    private static PlatformMessageStoreOptions ValidOptions()
    {
        return new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-orders",
            KafkaBootstrapServers = "kafka:9092",
            PostgresConnectionString = "Host=postgres;Database=integration;Username=app;Password=secret",
            InputTopic = "bip.integration-orders.in",
            DlqTopic = "bip.message.dlq",
            ReplayInputTopicPattern = "bip.%s.in",
            PayloadMode = PlatformMessageStoreOptions.PayloadModePostgresInline,
            PayloadMaxBytes = 1_048_576,
            PayloadRetentionDays = 30,
        };
    }
}
