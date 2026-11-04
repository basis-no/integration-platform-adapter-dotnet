using Confluent.Kafka;

namespace Basis.IntegrationPlatform.MessageStore;

// Wrap only the registered producer: application-owned producers have unknown
// coverage. Observable metrics keep exporter callbacks off the publish path.
internal sealed class PlatformTrafficProducer : IProducer<string, byte[]>
{
    private readonly IProducer<string, byte[]> inner;
    private readonly PlatformTrafficMetrics traffic;

    internal PlatformTrafficProducer(IProducer<string, byte[]> inner, PlatformTrafficMetrics traffic)
    {
        this.inner = inner;
        this.traffic = traffic;
    }

    // Enumeration of registrations can construct this wrapper even when the
    // effective DI producer is an application replacement. Only selection by
    // the consumer service establishes coverage of the registered producer.
    internal void Selected() => traffic.ProducerStarted();

    public Handle Handle => inner.Handle;
    public string Name => inner.Name;

    public async Task<DeliveryResult<string, byte[]>> ProduceAsync(string topic, Message<string, byte[]> message, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await inner.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
            Observe(topic, result);
            return result;
        }
        catch (ProduceException<string, byte[]> ex)
        {
            ObserveFailure(topic, ex.DeliveryResult);
            throw;
        }
        catch (OperationCanceledException)
        {
            // Cancellation may race a successful delivery; no ACK was observed.
            traffic.PublicationUncertain(topic);
            throw;
        }
    }

    public async Task<DeliveryResult<string, byte[]>> ProduceAsync(TopicPartition topicPartition, Message<string, byte[]> message, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await inner.ProduceAsync(topicPartition, message, cancellationToken).ConfigureAwait(false);
            Observe(topicPartition.Topic, result);
            return result;
        }
        catch (ProduceException<string, byte[]> ex)
        {
            ObserveFailure(topicPartition.Topic, ex.DeliveryResult);
            throw;
        }
        catch (OperationCanceledException)
        {
            traffic.PublicationUncertain(topicPartition.Topic);
            throw;
        }
    }

    public void Produce(string topic, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
        => inner.Produce(topic, message, report => { ObserveReport(topic, report); deliveryHandler?.Invoke(report); });

    public void Produce(TopicPartition topicPartition, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
        => inner.Produce(topicPartition, message, report => { ObserveReport(topicPartition.Topic, report); deliveryHandler?.Invoke(report); });

    private void ObserveReport(string topic, DeliveryReport<string, byte[]> report)
    {
        if (report.Error.IsError) ObserveFailure(topic, report);
        else Observe(topic, report);
    }

    private void Observe(string topic, DeliveryResult<string, byte[]> result)
    {
        if (!string.Equals(topic, result.Topic, StringComparison.Ordinal))
        {
            traffic.PublicationUncertain(topic);
            return;
        }
        traffic.RecordPublished(topic, result.Status, result.Partition.Value, result.Offset.Value);
    }

    private void ObserveFailure(string topic, DeliveryResult<string, byte[]> result)
    {
        // Known failure adds nothing. Ambiguous broker persistence makes the
        // signal unknown rather than claiming complete publication coverage.
        if (result.Status != PersistenceStatus.NotPersisted) traffic.PublicationUncertain(topic);
    }

    public int Poll(TimeSpan timeout) => inner.Poll(timeout);
    public int Flush(TimeSpan timeout) => inner.Flush(timeout);
    public void Flush(CancellationToken cancellationToken = default) => inner.Flush(cancellationToken);
    public int AddBrokers(string brokers) => inner.AddBrokers(brokers);
    public void SetSaslCredentials(string username, string password) => inner.SetSaslCredentials(username, password);
    public void InitTransactions(TimeSpan timeout) { traffic.InvalidatePublication(); inner.InitTransactions(timeout); }
    public void BeginTransaction() { traffic.InvalidatePublication(); inner.BeginTransaction(); }
    public void CommitTransaction(TimeSpan timeout) => inner.CommitTransaction(timeout);
    public void CommitTransaction() => inner.CommitTransaction();
    public void AbortTransaction(TimeSpan timeout) => inner.AbortTransaction(timeout);
    public void AbortTransaction() => inner.AbortTransaction();
    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        => inner.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
    public void Dispose()
    {
        traffic.ProducerStopped();
        inner.Dispose();
    }
}
