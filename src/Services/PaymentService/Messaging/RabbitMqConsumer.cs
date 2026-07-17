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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMq:HostName"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMq:Port"] ?? "5672"),
            UserName = _configuration["RabbitMq:UserName"] ?? "guest",
            Password = _configuration["RabbitMq:Password"] ?? "guest"
        };

        var exchangeName =
            _configuration["RabbitMq:ExchangeName"] ?? "orders.exchange";

        var queueName =
            _configuration["RabbitMq:QueueName"] ?? "payment.queue";

        var routingKey =
            _configuration["RabbitMq:RoutingKey"] ?? "OrderCreated";

        _logger.LogInformation(
            "Starting RabbitMQ consumer for queue {QueueName}",
            queueName);

        await using var connection =
            await factory.CreateConnectionAsync(stoppingToken);

        await using var channel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            cancellationToken: stoppingToken);

        // Process one unacknowledged message at a time.
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var json = Encoding.UTF8.GetString(ea.Body.ToArray());

            try
            {
                var order = JsonSerializer.Deserialize<OrderCreated>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                if (order is null)
                {
                    _logger.LogWarning(
                        "Discarding RabbitMQ message because it could not " +
                        "be deserialized into OrderCreated. Payload: {Payload}",
                        json);

                    await channel.BasicNackAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: false,
                        cancellationToken: stoppingToken);

                    return;
                }

                _logger.LogInformation(
                    "Received OrderCreated event for OrderId {OrderId} " +
                    "and CorrelationId {CorrelationId}",
                    order.OrderId,
                    order.CorrelationId);

                await using var scope =
                    _scopeFactory.CreateAsyncScope();

                var db = scope.ServiceProvider
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
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);

                _logger.LogInformation(
                    "Created payment {PaymentId} for OrderId {OrderId}",
                    payment.Id,
                    payment.OrderId);
            }
            catch (DbUpdateException ex)
                when (IsDuplicateKeyException(ex))
            {
                // The unique index on Payments.OrderId means that this
                // event has already been processed.
                _logger.LogInformation(
                    "OrderCreated event has already been processed. " +
                    "Acknowledging duplicate message.");

                await channel.BasicAckAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);
            }
            catch (JsonException ex)
            {
                // The message is invalid and retrying it will not fix it.
                _logger.LogError(
                    ex,
                    "Discarding malformed RabbitMQ message. Payload: {Payload}",
                    json);

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "RabbitMQ consumer is stopping.");
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Payment processing failed. Requeuing RabbitMQ message.");

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: true,
                    cancellationToken: stoppingToken);
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

    private static bool IsDuplicateKeyException(
        DbUpdateException exception)
    {
        return exception.InnerException is SqlException
        {
            Number: 2601 or 2627
        };
    }
}