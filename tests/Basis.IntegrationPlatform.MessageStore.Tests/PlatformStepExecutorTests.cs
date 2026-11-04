namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformStepExecutorTests
{
    [TestMethod]
    [DataRow("https://target.example/api/orders?orderId=42", "https://target.example/api/orders?orderId=42")]
    [DataRow("https://svc-user:synthetic-password@target.example/a", "https://xxxxxx@target.example/a")]
    [DataRow("https://synthetic-token@target.example/a", "https://xxxxxx@target.example/a")]
    [DataRow("https://svc-user:synthetic-password@target.example?token=t", "https://xxxxxx@target.example?token=xxxxxx")]
    [DataRow("//svc-user:synthetic-password@target.example/orders", "//xxxxxx@target.example/orders")]
    [DataRow("//target.example/orders?token=t", "//target.example/orders?token=xxxxxx")]
    [DataRow("/orders/user:pw@x?page=2", "/orders/user:pw@x?page=2")]
    [DataRow(
        "https://target.example/a?key=k&code=c&sig=s&Signature=s2&X-Amz-Credential=c2&X-Amz-Security-Token=t&client_secret=cs&password=p&ok=1",
        "https://target.example/a?key=xxxxxx&code=xxxxxx&sig=xxxxxx&Signature=xxxxxx&X-Amz-Credential=xxxxxx&X-Amz-Security-Token=xxxxxx&client_secret=xxxxxx&password=xxxxxx&ok=1")]
    [DataRow(
        "https://target.example/a?subscription-key=k&x-functions-key=k2&apiKey=k3&privatekey=p&passphrase=p2&pwd=p3&Authorization=Bearer%20t",
        "https://target.example/a?subscription-key=xxxxxx&x-functions-key=xxxxxx&apiKey=xxxxxx&privatekey=xxxxxx&passphrase=xxxxxx&pwd=xxxxxx&Authorization=xxxxxx")]
    [DataRow("https://target.example/a?client%5Fsecret=s&page=2", "https://target.example/a?client%5Fsecret=xxxxxx&page=2")]
    [DataRow("https://target.example/a?token=t#section", "https://target.example/a?token=xxxxxx#section")]
    [DataRow("https://target.example/a?token&page=2", "https://target.example/a?token&page=2")]
    [DataRow("https://target.example/files/user:pw@x?page=2", "https://target.example/files/user:pw@x?page=2")]
    [DataRow("orders-service", "orders-service")]
    public void MaskEndpointUriMasksUserInfoAndCredentialQueryValues(string endpointUri, string expected)
    {
        Assert.AreEqual(expected, PlatformStepExecutor.MaskEndpointUri(endpointUri));
    }
    [TestMethod]
    public async Task ContextOverloadWorksWithAnExistingInterfaceImplementation()
    {
        IPlatformStepExecutor steps = new LegacyExecutor();
        var message = new PlatformMessage("message-1", "correlation-1", "app", "test", "demo", "source", "target",
            "application/json", [], new Dictionary<string, string>());
        var calls = 0;
        var result = await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", null,
            (context, ct) =>
            {
                Assert.AreEqual("message-1:deliver", context.IdempotencyKey);
                Assert.AreEqual("deliver", context.StepId);
                Assert.AreEqual(CancellationToken.None, ct);
                calls++;
                return Task.CompletedTask;
            }, CancellationToken.None);
        Assert.AreEqual(1, calls);
        Assert.AreEqual("message-1:deliver", result.IdempotencyKey);
    }

    private sealed class LegacyExecutor : IPlatformStepExecutor
    {
        public async Task<PlatformStepExecutionResult> ExecuteAsync(PlatformMessage message, string stepId,
            string endpointUri, string? targetRef, Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
        {
            await operation(cancellationToken);
            return new PlatformStepExecutionResult($"{message.MessageId}:{stepId}", stepId, "DELIVERED", false);
        }
    }

}
