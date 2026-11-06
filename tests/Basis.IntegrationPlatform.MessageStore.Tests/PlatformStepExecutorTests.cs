namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformStepExecutorTests
{
    // Expected values are what the Camel adapter records for the same URI:
    // Camel 4.18's URISupport.sanitizeUri, which its step tracker applies.
    [TestMethod]
    [DataRow("https://target.example/api/orders?orderId=42", "https://target.example/api/orders?orderId=42")]
    [DataRow("//target.example/orders?token=t", "//target.example/orders?token=xxxxxx")]
    [DataRow("/orders/user:pw@x?page=2", "/orders/user:pw@x?page=2")]
    [DataRow("https://target.example/a?client%5Fsecret=s&page=2", "https://target.example/a?client%5Fsecret=xxxxxx&page=2")]
    [DataRow("https://target.example/a?token=t#section", "https://target.example/a?token=xxxxxx")]
    [DataRow("https://target.example/a?token&page=2", "https://target.example/a?token&page=xxxxxx")]
    [DataRow("https://target.example/files/user:pw@x?page=2", "https://target.example/files/user:xxxxxx@x?page=2")]
    [DataRow("orders-service", "orders-service")]
    [DataRow(
        "https://target.example/a?user=u&username=n&clientId=c&login=l&realm=r&emailAddress=e&page=2",
        "https://target.example/a?user=xxxxxx&username=xxxxxx&clientId=xxxxxx&login=xxxxxx&realm=xxxxxx&emailAddress=xxxxxx&page=2")]
    [DataRow(
        "https://target.example/a?accessToken=t&refresh_token=r&clientSecret=s&secretKey=k&page=2",
        "https://target.example/a?accessToken=xxxxxx&refresh_token=xxxxxx&clientSecret=xxxxxx&secretKey=xxxxxx&page=2")]
    [DataRow("https://target.example/a?password=RAW(p&q)&page=2", "https://target.example/a?password=xxxxxx&page=2")]
    [DataRow("https://target.example/a?password=RAW{p&q}&page=2", "https://target.example/a?password=xxxxxx&page=2")]
    [DataRow("https://target.example/a?PASSWORD=p&Token=t", "https://target.example/a?PASSWORD=xxxxxx&Token=xxxxxx")]
    [DataRow("https://target.example:8443/a?page=2", "https://target.example:8443/a?page=2")]
    [DataRow("https://target.example/a?access_token=t&page=2", "https://target.example/a?access_token=xxxxxx&page=2")]
    [DataRow("https://target.example/a?sessionToken=t&page=2", "https://target.example/a?sessionToken=xxxxxx&page=2")]
    [DataRow("https://target.example/a?userId=42&page=2", "https://target.example/a?userId=xxxxxx&page=2")]
    [DataRow("https://target.example/a?apikey=k&api_key=k2&page=2", "https://target.example/a?apikey=xxxxxx&api_key=xxxxxx&page=2")]
    [DataRow("https://target.example/a?page=2&passphrase=p", "https://target.example/a?page=2&passphrase=xxxxxx")]
    [DataRow("https://target.example/a?token=&page=2", "https://target.example/a?token=xxxxxx&page=2")]
    [DataRow(
        "http://target.example/cb?redirect=https://svc:pw@other.example/x",
        "http://target.example/cb?redirect=https://svc:xxxxxx@other.example/x")]
    // Java's '.' stops at each of these line terminators.
    [DataRow("https://target.example/a?redirect=https://a:secret@h\rhttps://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\rhttps://b:pw@h")]
    [DataRow("https://target.example/a?redirect=https://a:secret@h\nhttps://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\nhttps://b:pw@h")]
    [DataRow("https://target.example/a?redirect=https://a:secret@h\u0085https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u0085https://b:pw@h")]
    [DataRow("https://target.example/a?redirect=https://a:secret@h\u2028https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u2028https://b:pw@h")]
    [DataRow("https://target.example/a?redirect=https://a:secret@h\u2029https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u2029https://b:pw@h")]
    public void MaskEndpointUriRecordsWhatTheCamelAdapterRecords(string endpointUri, string expected)
    {
        Assert.AreEqual(expected, PlatformStepExecutor.MaskEndpointUri(endpointUri));
    }

    // This adapter masks more than the Camel adapter: the whole user info, and
    // the DLQ header rule's names plus key, code, sig and pwd. The Camel
    // adapter records the third value.
    [TestMethod]
    [DataRow(
        "https://svc-user:synthetic-password@target.example/a",
        "https://xxxxxx@target.example/a",
        "https://svc-user:xxxxxx@target.example/a")]
    [DataRow("https://synthetic-token@target.example/a", "https://xxxxxx@target.example/a", "https://synthetic-token@target.example/a")]
    [DataRow(
        "https://svc-user:synthetic-password@target.example?token=t",
        "https://xxxxxx@target.example?token=xxxxxx",
        "https://svc-user:xxxxxx@target.example?token=xxxxxx")]
    [DataRow(
        "//svc-user:synthetic-password@target.example/orders",
        "//xxxxxx@target.example/orders",
        "//svc-user:synthetic-password@target.example/orders")]
    [DataRow("https://svc-user@target.example:8443/a", "https://xxxxxx@target.example:8443/a", "https://svc-user@target.example:8443/a")]
    [DataRow(
        "https://target.example/a?key=k&code=c&sig=s&Signature=s2&X-Amz-Credential=c2&X-Amz-Security-Token=t&client_secret=cs&password=p&ok=1",
        "https://target.example/a?key=xxxxxx&code=xxxxxx&sig=xxxxxx&Signature=xxxxxx&X-Amz-Credential=xxxxxx&X-Amz-Security-Token=xxxxxx&client_secret=xxxxxx&password=xxxxxx&ok=1",
        "https://target.example/a?key=k&code=c&sig=s&Signature=s2&X-Amz-Credential=c2&X-Amz-Security-Token=xxxxxx&client_secret=xxxxxx&password=xxxxxx&ok=1")]
    [DataRow(
        "https://target.example/a?subscription-key=k&x-functions-key=k2&apiKey=k3&privatekey=p&passphrase=p2&pwd=p3&Authorization=Bearer%20t",
        "https://target.example/a?subscription-key=xxxxxx&x-functions-key=xxxxxx&apiKey=xxxxxx&privatekey=xxxxxx&passphrase=xxxxxx&pwd=xxxxxx&Authorization=xxxxxx",
        "https://target.example/a?subscription-key=k&x-functions-key=k2&apiKey=xxxxxx&privatekey=xxxxxx&passphrase=xxxxxx&pwd=p3&Authorization=Bearer%20t")]
    [DataRow("https://target.example/a?code=c&page=2", "https://target.example/a?code=xxxxxx&page=2", "https://target.example/a?code=c&page=2")]
    [DataRow(
        "https://target.example/a?sig=s&sv=2024&page=2",
        "https://target.example/a?sig=xxxxxx&sv=2024&page=2",
        "https://target.example/a?sig=s&sv=2024&page=2")]
    // Camel's user info pattern can reach past '?'; each rule still reads the
    // URI as given, so neither replacement hides a value from the other.
    [DataRow(
        "https://a:pw?x=y@h&code=synthetic-secret",
        "https://a:xxxxxx@h&code=xxxxxx",
        "https://a:xxxxxx@h&code=synthetic-secret")]
    [DataRow("https://a:b@h/x?u=c:d@e", "https://xxxxxx@e", "https://a:xxxxxx@e")]
    [DataRow(
        "https://u:p@h/cb?r=https://x:y@z",
        "https://xxxxxx@h/cb?r=https://x:xxxxxx@z",
        "https://u:p@h/cb?r=https://x:xxxxxx@z")]
    public void MaskEndpointUriAlsoMasksUserInfoAndTheAdapterCredentialNames(string endpointUri, string expected, string camelRecords)
    {
        Assert.AreEqual(expected, PlatformStepExecutor.MaskEndpointUri(endpointUri));
        Assert.AreNotEqual(camelRecords, expected);
    }

    [TestMethod]
    public void MaskEndpointUriRecordsNothingOfAUriThePatternsCannotMaskInTime()
    {
        var pathological = "https://target.example/a?" + string.Concat(Enumerable.Repeat("&token", 20_000));

        Assert.AreEqual("xxxxxx", PlatformStepExecutor.MaskEndpointUri(pathological));
    }

    [TestMethod]
    public async Task ContextOverloadWorksWithAnExistingInterfaceImplementation()
    {
        IPlatformStepExecutor steps = new LegacyExecutor();
        var message = new PlatformMessage("message-1", "correlation-1", "app", "test", "demo", "source", "target",
            "application/json", [], new Dictionary<string, string>());
        var calls = 0;
        var result = await steps.ExecuteWithContextAsync(message, "deliver", "https://target.example/orders", null,
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

    // Compile-time check: every call shape that compiled against ExecuteAsync
    // still binds to it. The calls are never run.
    [TestMethod]
    public void ExistingExecuteAsyncCallShapesStillCompile()
    {
        IPlatformStepExecutor steps = new LegacyExecutor();
        var message = new PlatformMessage("message-1", "correlation-1", "app", "test", "demo", "source", "target",
            "application/json", [], new Dictionary<string, string>());
        Func<CancellationToken, Task> typed = _ => Task.CompletedTask;
        Func<Task>[] calls =
        [
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, ct => Task.CompletedTask, CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, async ct => await Task.Delay(0, ct), CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, delegate { return Task.CompletedTask; }, CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, SendAsync, CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, typed, CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", targetRef: null, operation: _ => Task.CompletedTask, cancellationToken: CancellationToken.None),
            () => steps.ExecuteAsync(message, "s", "https://target.example/a", null, null!, CancellationToken.None),
        ];

        Assert.AreEqual(7, calls.Length);

        static Task SendAsync(CancellationToken cancellationToken) => Task.CompletedTask;
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
