using System.Text;
using System.Text.Json;
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
        Console.WriteLine("***** RabbitMqConsumer starting *****");

        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMq:HostName"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMq:Port"] ?? "5672"),
            UserName = _configuration["RabbitMq:UserName"] ?? "guest",
            Password = _configuration["RabbitMq:Password"] ?? "guest"
        };

        await using var connection = await factory.CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        var exchangeName = _configuration["RabbitMq:ExchangeName"] ?? "orders.exchange";
        var queueName = _configuration["RabbitMq:QueueName"] ?? "payment.queue";
        var routingKey = _configuration["RabbitMq:RoutingKey"] ?? "OrderCreated";

        Console.WriteLine($"Exchange    : {exchangeName}");
        Console.WriteLine($"Queue       : {queueName}");
        Console.WriteLine($"Routing Key : {routingKey}");

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: stoppingToken);

        Console.WriteLine("Exchange declared.");

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        Console.WriteLine("Queue declared.");

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            cancellationToken: stoppingToken);

        Console.WriteLine("Queue bound to exchange.");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            Console.WriteLine("***** PaymentService received a RabbitMQ message *****");

            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());

                Console.WriteLine(json);

                var order = JsonSerializer.Deserialize<OrderCreated>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                if (order == null)
                {
                    Console.WriteLine("Deserialization returned null.");

                    await channel.BasicNackAsync(
                        ea.DeliveryTag,
                        false,
                        false,
                        stoppingToken);

                    return;
                }

                Console.WriteLine($"Processing Order {order.OrderId}");

                using var scope = _scopeFactory.CreateScope();

                var db = scope.ServiceProvider.GetRequiredService<PaymentDbContext>();

                var alreadyExists = await db.Payments.AnyAsync(
                    p => p.OrderId == order.OrderId,
                    stoppingToken);

                if (alreadyExists)
                {
                    Console.WriteLine("Payment already exists.");

                    await channel.BasicAckAsync(
                        ea.DeliveryTag,
                        false,
                        stoppingToken);

                    return;
                }

                db.Payments.Add(new Payment
                {
                    Id = Guid.NewGuid(),
                    OrderId = order.OrderId,
                    Amount = order.TotalAmount,
                    Status = "Succeeded",
                    CreatedAtUtc = DateTime.UtcNow
                });

                await db.SaveChangesAsync(stoppingToken);

                Console.WriteLine("Payment saved.");

                await channel.BasicAckAsync(
                    ea.DeliveryTag,
                    false,
                    stoppingToken);

                Console.WriteLine("Message acknowledged.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);

                await channel.BasicNackAsync(
                    ea.DeliveryTag,
                    false,
                    true,
                    stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        Console.WriteLine("***** PaymentService is listening on payment.queue *****");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}