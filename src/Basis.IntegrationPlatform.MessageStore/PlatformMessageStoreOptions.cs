using Npgsql;
using Confluent.Kafka;

namespace Basis.IntegrationPlatform.MessageStore;

public sealed class PlatformMessageStoreOptions
{
    public const string PayloadModePostgresInline = "postgres_inline";
    public const string PayloadModeMetadataOnly = "metadata_only";

    public string ApplicationName { get; set; } = Environment.GetEnvironmentVariable("APP_NAME") ?? "dotnet-integration";
    public string EnvironmentName { get; set; } = Environment.GetEnvironmentVariable("APP_ENVIRONMENT") ?? "local";
    public string Tenant { get; set; } = Environment.GetEnvironmentVariable("APP_TENANT") ?? "demo";
    public string KafkaBootstrapServers { get; set; } = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "";
    public SecurityProtocol KafkaSecurityProtocol { get; set; } = EnumFromEnvironment("BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL", SecurityProtocol.Plaintext);
    public SaslMechanism KafkaSaslMechanism { get; set; } = EnumFromEnvironment("BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM", SaslMechanism.ScramSha512);
    public string? KafkaSaslUsername { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_KAFKA_SASL_USERNAME");
    public string? KafkaSaslPassword { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_KAFKA_SASL_PASSWORD");
    public string? KafkaSslCaLocation { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_KAFKA_SSL_CA_LOCATION");
    public string InputTopic { get; set; } = Environment.GetEnvironmentVariable("APP_KAFKA_INPUT_TOPIC") ?? "";
    public string DlqTopic { get; set; } = Environment.GetEnvironmentVariable("APP_KAFKA_DLQ_TOPIC") ?? "bip.message.dlq";
    public string PostgresConnectionString { get; set; } = ResolvePostgresConnectionString();
    public string ConsumerGroupId { get; set; } = Environment.GetEnvironmentVariable("APP_NAME") ?? "dotnet-integration";
    public string RouteId { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_ROUTE_ID") ?? "dotnet-handler";
    public string ReplayInputTopicPattern { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN") ?? "bip.%s.in";
    public int MaxHandlerAttempts { get; set; } = IntFromEnvironment("BASIS_MESSAGE_STORE_MAX_HANDLER_ATTEMPTS", 3);
    public int HandlerRetryDelayMs { get; set; } = IntFromEnvironment("BASIS_MESSAGE_STORE_HANDLER_RETRY_DELAY_MS", 1_000);
    public string PayloadMode { get; set; } = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_PAYLOAD_MODE") ?? PayloadModePostgresInline;
    public long PayloadMaxBytes { get; set; } = LongFromEnvironment("BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES", 1_048_576);
    public int PayloadRetentionDays { get; set; } = IntFromEnvironment("BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS", 30);

    /// Contract gate (spec section 10): bounded retries before degrading to a
    /// warning when the version cannot be read. Not env-driven; tests may lower.
    public int ContractGateMaxAttempts { get; set; } = 10;
    public int ContractGateRetryDelayMs { get; set; } = 3_000;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ApplicationName))
        {
            throw new InvalidOperationException("APP_NAME is required for the Basis message-store adapter.");
        }
        if (string.IsNullOrWhiteSpace(KafkaBootstrapServers))
        {
            throw new InvalidOperationException("KAFKA_BOOTSTRAP_SERVERS is required for the Basis message-store adapter.");
        }
        if (!Enum.IsDefined(KafkaSecurityProtocol) || !Enum.IsDefined(KafkaSaslMechanism))
        {
            throw new InvalidOperationException("Invalid Kafka security configuration.");
        }
        if (KafkaSecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.SaslPlaintext)
        {
            if (KafkaSaslMechanism is not (SaslMechanism.ScramSha512 or SaslMechanism.ScramSha256 or SaslMechanism.Plain)
                || string.IsNullOrWhiteSpace(KafkaSaslUsername) || string.IsNullOrWhiteSpace(KafkaSaslPassword))
            {
                throw new InvalidOperationException("Kafka SASL requires a supported mechanism and credentials.");
            }
        }
        if (string.IsNullOrWhiteSpace(InputTopic))
        {
            InputTopic = $"bip.{ApplicationName}.in";
        }
        if (string.IsNullOrWhiteSpace(DlqTopic))
        {
            DlqTopic = "bip.message.dlq";
        }
        if (string.IsNullOrWhiteSpace(RouteId))
        {
            RouteId = "dotnet-handler";
        }
        if (string.IsNullOrWhiteSpace(ReplayInputTopicPattern))
        {
            ReplayInputTopicPattern = "bip.%s.in";
        }
        if (MaxHandlerAttempts < 1)
        {
            throw new InvalidOperationException("BASIS_MESSAGE_STORE_MAX_HANDLER_ATTEMPTS must be at least 1.");
        }
        if (HandlerRetryDelayMs < 0)
        {
            throw new InvalidOperationException("BASIS_MESSAGE_STORE_HANDLER_RETRY_DELAY_MS must be at least 0.");
        }
        PayloadMode = NormalizePayloadMode(PayloadMode);
        if (PayloadMaxBytes < 0)
        {
            throw new InvalidOperationException("BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES must be at least 0.");
        }
        if (PayloadRetentionDays < 0)
        {
            throw new InvalidOperationException("BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS must be at least 0.");
        }
        if (string.IsNullOrWhiteSpace(PostgresConnectionString))
        {
            throw new InvalidOperationException("POSTGRES_JDBC_URL or POSTGRES_CONNECTION_STRING is required for the Basis message-store adapter.");
        }
    }

    internal ClientConfig CreateKafkaClientConfig()
    {
        Validate();
        var config = new ClientConfig
        {
            BootstrapServers = KafkaBootstrapServers,
            SecurityProtocol = KafkaSecurityProtocol,
        };
        if (KafkaSecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.SaslPlaintext)
        {
            config.SaslMechanism = KafkaSaslMechanism;
            config.SaslUsername = KafkaSaslUsername;
            config.SaslPassword = KafkaSaslPassword;
        }
        if (KafkaSecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.Ssl)
        {
            config.SslCaLocation = KafkaSslCaLocation;
            config.EnableSslCertificateVerification = true;
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
        }
        return config;
    }

    private static T EnumFromEnvironment<T>(string name, T fallback) where T : struct, Enum
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        var normalized = raw.Replace("_", "").Replace("-", "");
        if (Enum.TryParse<T>(normalized, true, out var value) && Enum.IsDefined(value)) return value;
        throw new InvalidOperationException($"Invalid {name} configuration.");
    }

    private static string NormalizePayloadMode(string? value)
    {
        var normalized = string.IsNullOrWhiteSpace(value)
            ? PayloadModePostgresInline
            : value.Trim().ToLowerInvariant().Replace('-', '_');
        if (normalized is not (PayloadModePostgresInline or PayloadModeMetadataOnly))
        {
            throw new InvalidOperationException(
                $"Unsupported BASIS_MESSAGE_STORE_PAYLOAD_MODE value: {value}. " +
                $"Supported values: {PayloadModePostgresInline}, {PayloadModeMetadataOnly}.");
        }

        return normalized;
    }

    private static string ResolvePostgresConnectionString()
    {
        var direct = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");
        if (!string.IsNullOrWhiteSpace(direct))
        {
            return direct;
        }

        var jdbc = Environment.GetEnvironmentVariable("POSTGRES_JDBC_URL");
        var username = Environment.GetEnvironmentVariable("POSTGRES_USERNAME");
        var password = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        if (string.IsNullOrWhiteSpace(jdbc))
        {
            return "";
        }

        var uri = new Uri(jdbc.Replace("jdbc:", "", StringComparison.OrdinalIgnoreCase));
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = uri.Host,
            Port = uri.Port > 0 ? uri.Port : 5432,
            Database = uri.AbsolutePath.Trim('/'),
            Username = username,
            Password = password,
        };
        var poolSize = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_POSTGRES_MAX_POOL_SIZE");
        if (poolSize is not null)
        {
            if (!int.TryParse(poolSize, out var maxPoolSize) || maxPoolSize < 1 || maxPoolSize > 1000)
                throw new InvalidOperationException("Invalid BASIS_MESSAGE_STORE_POSTGRES_MAX_POOL_SIZE configuration.");
            builder.MaxPoolSize = maxPoolSize;
        }
        builder.SslMode = EnumFromEnvironment("BASIS_MESSAGE_STORE_POSTGRES_SSL_MODE", builder.SslMode);
        var rootCertificate = Environment.GetEnvironmentVariable("BASIS_MESSAGE_STORE_POSTGRES_ROOT_CERTIFICATE");
        if (!string.IsNullOrWhiteSpace(rootCertificate)) builder.RootCertificate = rootCertificate;
        return builder.ConnectionString;
    }

    private static int IntFromEnvironment(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return int.TryParse(value, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }

    private static long LongFromEnvironment(string name, long defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return long.TryParse(value, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }
}
