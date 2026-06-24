namespace OrderService.Models;

public class ProcessedMessage
{
    public Guid MessageId { get; set; }

    public string ConsumerName { get; set; } = string.Empty;

    public DateTime ProcessedAtUtc { get; set; } = DateTime.UtcNow;
}