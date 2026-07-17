using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using PaymentService.Data;
using PaymentService.Entities;
using PaymentService.Events;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PaymentService.Messaging;

public sealed class RabbitMqConsumer : BackgroundService
{
    private const string RetryCountHeader = "x-retry-count";

    private readonly IConfiguration _configuration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<RabbitMqConsumer> _logger;

    public RabbitMqConsumer(
        IConfiguration configuration,
        IServiceScopeFactory scopeFactory,
        ILogger<RabbitMqConsumer> logger)
    {
        _configuration = configuration;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName =
                _configuration["RabbitMq:HostName"] ?? "localhost",

            Port = int.Parse(
                _configuration["RabbitMq:Port"] ?? "5672"),

            UserName =
                _configuration["RabbitMq:UserName"] ?? "guest",

            Password =
                _configuration["RabbitMq:Password"] ?? "guest",

            AutomaticRecoveryEnabled = true
        };

        var exchangeName =
            _configuration["RabbitMq:ExchangeName"]
            ?? "orders.exchange";

        var queueName =
            _configuration["RabbitMq:QueueName"]
            ?? "payment.queue";

        var routingKey =
            _configuration["RabbitMq:RoutingKey"]
            ?? "OrderCreated";

        var retryExchangeName =
            _configuration["RabbitMq:RetryExchangeName"]
            ?? "orders.retry.exchange";

        var retryQueueName =
            _configuration["RabbitMq:RetryQueueName"]
            ?? "payment.retry.queue";

        var retryRoutingKey =
            _configuration["RabbitMq:RetryRoutingKey"]
            ?? "payment.retry";

        var retryDelayMilliseconds =
            int.Parse(
                _configuration["RabbitMq:RetryDelayMilliseconds"]
                ?? "10000");

        var maximumRetryCount =
            int.Parse(
                _configuration["RabbitMq:MaximumRetryCount"]
                ?? "3");

        var deadLetterExchangeName =
            _configuration["RabbitMq:DeadLetterExchangeName"]
            ?? "orders.deadletter.exchange";

        var deadLetterQueueName =
            _configuration["RabbitMq:DeadLetterQueueName"]
            ?? "payment.deadletter.queue";

        var deadLetterRoutingKey =
            _configuration["RabbitMq:DeadLetterRoutingKey"]
            ?? "payment.failed";

        _logger.LogInformation(
            "Starting RabbitMQ consumer for queue {QueueName}",
            queueName);

        await using var connection =
            await factory.CreateConnectionAsync(stoppingToken);

        await using var channel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        // Main OrderService exchange.
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Retry exchange.
        await channel.ExchangeDeclareAsync(
            exchange: retryExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Final dead-letter exchange.
        await channel.ExchangeDeclareAsync(
            exchange: deadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // Final dead-letter queue.
        await channel.QueueDeclareAsync(
            queue: deadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: deadLetterQueueName,
            exchange: deadLetterExchangeName,
            routingKey: deadLetterRoutingKey,
            cancellationToken: stoppingToken);

        /*
         * Messages stay in this queue for the configured delay.
         * When they expire, RabbitMQ sends them back to orders.exchange
         * using the OrderCreated routing key.
         */
        var retryQueueArguments = new Dictionary<string, object?>
        {
            ["x-message-ttl"] = retryDelayMilliseconds,
            ["x-dead-letter-exchange"] = exchangeName,
            ["x-dead-letter-routing-key"] = routingKey
        };

        await channel.QueueDeclareAsync(
            queue: retryQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryQueueArguments,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: retryQueueName,
            exchange: retryExchangeName,
            routingKey: retryRoutingKey,
            cancellationToken: stoppingToken);

        // Permanent failures from the main queue go to the final DLQ.
        var paymentQueueArguments = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] =
                deadLetterExchangeName,

            ["x-dead-letter-routing-key"] =
                deadLetterRoutingKey
        };

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: paymentQueueArguments,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            cancellationToken: stoppingToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var json = Encoding.UTF8.GetString(body);

            OrderCreated? order = null;

            try
            {
                order = JsonSerializer.Deserialize<OrderCreated>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                if (order is null)
                {
                    _logger.LogWarning(
                        "Message could not be deserialized into " +
                        "OrderCreated. Moving it to the dead-letter queue. " +
                        "Payload: {Payload}",
                        json);

                    await RejectToDeadLetterQueueAsync(
                        channel,
                        eventArgs.DeliveryTag,
                        stoppingToken);

                    return;
                }

                using var loggingScope =
                    _logger.BeginScope(
                        new Dictionary<string, object>
                        {
                            ["OrderId"] = order.OrderId,
                            ["CorrelationId"] =
                                order.CorrelationId
                        });

                var currentRetryCount =
                    GetRetryCount(eventArgs.BasicProperties);

                _logger.LogInformation(
                    "Received OrderCreated event. Retry count: " +
                    "{RetryCount}",
                    currentRetryCount);

                await using var serviceScope =
                    _scopeFactory.CreateAsyncScope();

                var db = serviceScope.ServiceProvider
                    .GetRequiredService<PaymentDbContext>();

                var payment = new Payment
                {
                    Id = Guid.NewGuid(),
                    OrderId = order.OrderId,
                    Amount = order.TotalAmount,
                    Status = "Succeeded",
                    CreatedAtUtc = DateTime.UtcNow
                };

                db.Payments.Add(payment);

                await db.SaveChangesAsync(stoppingToken);

                await channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);

                _logger.LogInformation(
                    "Created payment {PaymentId} for amount " +
                    "{PaymentAmount}",
                    payment.Id,
                    payment.Amount);
            }
            catch (DbUpdateException exception)
                when (IsDuplicateKeyException(exception))
            {
                _logger.LogInformation(
                    "OrderCreated event was already processed. " +
                    "Acknowledging duplicate message.");

                await channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);
            }
            catch (JsonException exception)
            {
                _logger.LogError(
                    exception,
                    "Malformed RabbitMQ message. Moving it directly " +
                    "to the dead-letter queue. Payload: {Payload}",
                    json);

                await RejectToDeadLetterQueueAsync(
                    channel,
                    eventArgs.DeliveryTag,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "RabbitMQ message processing was cancelled.");
            }
            catch (Exception exception)
            {
                var currentRetryCount =
                    GetRetryCount(eventArgs.BasicProperties);

                if (currentRetryCount < maximumRetryCount)
                {
                    var nextRetryCount =
                        currentRetryCount + 1;

                    _logger.LogWarning(
                        exception,
                        "Payment processing failed. Scheduling retry " +
                        "{RetryCount} of {MaximumRetryCount} after " +
                        "{RetryDelayMilliseconds} milliseconds.",
                        nextRetryCount,
                        maximumRetryCount,
                        retryDelayMilliseconds);

                    try
                    {
                        await PublishForRetryAsync(
                            channel,
                            retryExchangeName,
                            retryRoutingKey,
                            body,
                            eventArgs.BasicProperties,
                            nextRetryCount,
                            stoppingToken);

                        /*
                         * Only acknowledge the original message after
                         * the retry copy has been published.
                         */
                        await channel.BasicAckAsync(
                            deliveryTag:
                                eventArgs.DeliveryTag,
                            multiple: false,
                            cancellationToken:
                                stoppingToken);
                    }
                    catch (Exception publishException)
                    {
                        _logger.LogError(
                            publishException,
                            "Could not publish the message to the retry " +
                            "exchange. Requeueing the original message.");

                        await channel.BasicNackAsync(
                            deliveryTag:
                                eventArgs.DeliveryTag,
                            multiple: false,
                            requeue: true,
                            cancellationToken:
                                stoppingToken);
                    }
                }
                else
                {
                    _logger.LogError(
                        exception,
                        "Payment processing failed after " +
                        "{MaximumRetryCount} retries. Moving message " +
                        "to the dead-letter queue.",
                        maximumRetryCount);

                    await RejectToDeadLetterQueueAsync(
                        channel,
                        eventArgs.DeliveryTag,
                        stoppingToken);
                }
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation(
            "RabbitMQ consumer is listening on queue {QueueName}",
            queueName);

        try
        {
            await Task.Delay(
                Timeout.InfiniteTimeSpan,
                stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "RabbitMQ consumer stopped.");
        }
    }

    private static async Task PublishForRetryAsync(
        IChannel channel,
        string retryExchangeName,
        string retryRoutingKey,
        byte[] body,
        IReadOnlyBasicProperties originalProperties,
        int retryCount,
        CancellationToken cancellationToken)
    {
        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = originalProperties.ContentType
                ?? "application/json",
            CorrelationId = originalProperties.CorrelationId,
            MessageId = originalProperties.MessageId,
            Type = originalProperties.Type,
            Headers = CopyHeaders(originalProperties.Headers)
        };

        properties.Headers[RetryCountHeader] = retryCount;

        await channel.BasicPublishAsync(
            exchange: retryExchangeName,
            routingKey: retryRoutingKey,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);
    }

    private static Dictionary<string, object?> CopyHeaders(
        IDictionary<string, object?>? originalHeaders)
    {
        return originalHeaders is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(
                originalHeaders);
    }

    private static int GetRetryCount(
        IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null ||
            !properties.Headers.TryGetValue(
                RetryCountHeader,
                out var value) ||
            value is null)
        {
            return 0;
        }

        return value switch
        {
            byte byteValue => byteValue,
            short shortValue => shortValue,
            int intValue => intValue,
            long longValue => checked((int)longValue),
            byte[] bytes when int.TryParse(
                Encoding.UTF8.GetString(bytes),
                out var parsedValue) => parsedValue,
            _ => 0
        };
    }

    private static async Task RejectToDeadLetterQueueAsync(
        IChannel channel,
        ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        await channel.BasicNackAsync(
            deliveryTag: deliveryTag,
            multiple: false,
            requeue: false,
            cancellationToken: cancellationToken);
    }

    private static bool IsDuplicateKeyException(
        DbUpdateException exception)
    {
        return exception.InnerException is SqlException
        {
            Number: 2601 or 2627
        };
    }
}