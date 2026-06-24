namespace OrderService.Requests;

public class CreateOrderRequest
{
    public string CustomerName { get; set; } = string.Empty;

    public string CustomerEmail { get; set; } = string.Empty;

    public string PackageName { get; set; } = string.Empty;

    public decimal TotalAmount { get; set; }
}