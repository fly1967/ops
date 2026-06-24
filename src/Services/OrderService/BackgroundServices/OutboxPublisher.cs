using Microsoft.EntityFrameworkCore;
using OrderService.Data;

namespace OrderService.BackgroundServices;

public class OutboxPublisher : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(
        IServiceScopeFactory scopeFactory,
        ILogger<OutboxPublisher> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Publisher started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();

                var db =
                    scope.ServiceProvider
                         .GetRequiredService<OrdersDbContext>();

                var messages = await db.OutboxMessages
                    .Where(x => !x.Published)
                    .OrderBy(x => x.CreatedAtUtc)
                    .ToListAsync(stoppingToken);

                foreach (var message in messages)
                {
                    _logger.LogInformation(
                        "Publishing EventType={EventType}, MessageId={MessageId}",
                        message.EventType,
                        message.Id);

                    // RabbitMQ will go here later

                    message.Published = true;
                }

                if (messages.Any())
                {
                    await db.SaveChangesAsync(stoppingToken);

                    _logger.LogInformation(
                        "Published {Count} outbox messages",
                        messages.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error while processing outbox messages");
            }

            await Task.Delay(
                TimeSpan.FromSeconds(5),
                stoppingToken);
        }
    }
}