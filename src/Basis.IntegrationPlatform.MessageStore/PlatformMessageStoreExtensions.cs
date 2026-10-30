using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Text.Json;

namespace Basis.IntegrationPlatform.MessageStore;

public static class PlatformMessageStoreExtensions
{
    /// <summary>Key for the adapter datasource, separate from application data.</summary>
    public const string MessageStoreDataSourceKey = "basis-message-store";

    private static readonly JsonSerializerOptions ReplayJsonOptions = new(JsonSerializerDefaults.Web);

    public static IServiceCollection AddBasisMessageStoreAdapter<THandler>(
        this IServiceCollection services,
        Action<PlatformMessageStoreOptions>? configure = null)
        where THandler : class, IPlatformMessageHandler
    {
        services.AddOptions<PlatformMessageStoreOptions>().Configure(options => configure?.Invoke(options));
        services.AddKeyedSingleton<NpgsqlDataSource>(MessageStoreDataSourceKey, (provider, _) =>
        {
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<PlatformMessageStoreOptions>>().Value;
            options.Validate();
            return NpgsqlDataSource.Create(options.PostgresConnectionString);
        });
        services.AddSingleton<PlatformStepObservationCollector>();
        // Preserve the original default datasource registration for compatibility.
        // Adapter internals always select their own key, even if the application
        // registers a different default datasource for business data afterwards.
        services.AddSingleton(provider => provider.GetRequiredKeyedService<NpgsqlDataSource>(MessageStoreDataSourceKey));
        services.AddSingleton(provider => new PlatformMessageStore(
            provider.GetRequiredKeyedService<NpgsqlDataSource>(MessageStoreDataSourceKey),
            provider.GetRequiredService<PlatformStepObservationCollector>(),
            provider.GetRequiredService<IOptions<PlatformMessageStoreOptions>>(),
            provider.GetRequiredService<ILogger<PlatformMessageStore>>()));
        services.AddSingleton<IPlatformStepExecutor>(provider => new PlatformStepExecutor(
            provider.GetRequiredKeyedService<NpgsqlDataSource>(MessageStoreDataSourceKey),
            provider.GetRequiredService<PlatformStepObservationCollector>(),
            provider.GetRequiredService<ILogger<PlatformStepExecutor>>()));
        services.AddSingleton<IProducer<string, byte[]>>(provider =>
        {
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<PlatformMessageStoreOptions>>().Value;
            return new ProducerBuilder<string, byte[]>(new ProducerConfig(options.CreateKafkaClientConfig())).Build();
        });
        services.AddSingleton<IPlatformMessageHandler, THandler>();
        services.AddHostedService<PlatformMessageConsumerService>();
        return services;
    }

    public static IEndpointRouteBuilder MapBasisMessageStoreAdminEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost("/admin/messages/{messageId}/resend", async (
            string messageId,
            HttpContext context,
            PlatformMessageStore store,
            IProducer<string, byte[]> producer,
            CancellationToken cancellationToken) =>
        {
            var (request, parseError) = await ReadReplayRequestAsync(context.Request.Body, cancellationToken);
            if (parseError is not null)
            {
                return Results.BadRequest(new ReplayErrorBody("BAD_REQUEST", parseError));
            }
            request ??= new MessageReplayRequest(
                context.Request.Headers["x-dashboard-user"].ToString(),
                context.Request.Headers["x-dashboard-reason"].ToString(),
                []);
            var result = await store.ReplayAsync(messageId, request, producer, cancellationToken);
            return result.ErrorCode switch
            {
                null when result.Status == "PUBLISHED" =>
                    Results.Accepted($"/admin/messages/{messageId}/resend", result),
                MessageReplayErrorCodes.NotFound =>
                    Results.NotFound(new ReplayErrorBody(MessageReplayErrorCodes.NotFound, result.Error ?? "Message not found.")),
                MessageReplayErrorCodes.NotReplayable =>
                    Results.Conflict(new ReplayErrorBody(MessageReplayErrorCodes.NotReplayable, result.Error ?? "Message is not replayable.", result.ReplayId)),
                MessageReplayErrorCodes.PublishFailed =>
                    Results.Json(
                        new ReplayErrorBody(MessageReplayErrorCodes.PublishFailed, result.Error ?? "Replay publish failed.", result.ReplayId),
                        statusCode: StatusCodes.Status500InternalServerError),
                _ =>
                    Results.Json(
                        new ReplayErrorBody(result.ErrorCode ?? MessageReplayErrorCodes.PublishFailed, result.Error ?? "Replay failed.", result.ReplayId),
                        statusCode: StatusCodes.Status500InternalServerError),
            };
        });
        return endpoints;
    }

    /// <summary>
    /// Reads the optional resend request body. The body is buffered and parsed
    /// whenever any bytes are actually present — never gated on Content-Length,
    /// which is absent for chunked transfer encoding. Silently dropping a
    /// chunked body carrying redoSteps would be the silent-redo-noop that
    /// contract v1 §9 forbids. An empty body yields no request (header
    /// fallback applies); a malformed body yields a BAD_REQUEST error.
    /// </summary>
    internal static async Task<(MessageReplayRequest? Request, string? Error)> ReadReplayRequestAsync(
        Stream body,
        CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        await body.CopyToAsync(buffer, cancellationToken);
        if (buffer.Length == 0)
        {
            return (null, null);
        }

        buffer.Position = 0;
        try
        {
            var request = await JsonSerializer.DeserializeAsync<MessageReplayRequest>(buffer, ReplayJsonOptions, cancellationToken);
            return (request, null);
        }
        catch (JsonException ex)
        {
            return (null, $"Malformed replay request body: {ex.Message}");
        }
    }
}

/// <summary>
/// Error body of the resend endpoint per spec/openapi/resend-api.v1.yaml:
/// { code, error, replayId? }.
/// </summary>
public sealed record ReplayErrorBody(string Code, string Error, string? ReplayId = null);
