using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Messaging;

namespace OrderService.BackgroundServices;

public sealed class OutboxPublisher : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly RabbitMqPublisher _rabbitMqPublisher;

    public OutboxPublisher(
        IServiceScopeFactory scopeFactory,
        ILogger<OutboxPublisher> logger,
        RabbitMqPublisher rabbitMqPublisher)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _rabbitMqPublisher = rabbitMqPublisher;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Publisher started.");
        Console.WriteLine("***** RabbitMqConsumer starting *****");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();

                var dbContext = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

                var messages = await dbContext.OutboxMessages
                    .Where(m => m.PublishedAtUtc == null)
                    .OrderBy(m => m.CreatedAtUtc)
                    .Take(10)
                    .ToListAsync(stoppingToken);

                foreach (var outboxMessage in messages)
                {
                    try
                    {
                        await _rabbitMqPublisher.PublishAsync(
                            outboxMessage.EventType,
                            outboxMessage.Payload,
                            stoppingToken);

                        outboxMessage.PublishedAtUtc = DateTime.UtcNow;
                        outboxMessage.Published = true;

                        _logger.LogInformation(
                            "Published Outbox Message {MessageId}",
                            outboxMessage.Id);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to publish Outbox Message {MessageId}",
                            outboxMessage.Id);
                    }
                }

                await dbContext.SaveChangesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox messages.");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }
}