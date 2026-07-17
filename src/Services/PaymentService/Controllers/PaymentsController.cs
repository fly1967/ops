using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using PaymentService.Data;

namespace PaymentService.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class PaymentsController : ControllerBase
{
    private readonly PaymentDbContext _db;

    public PaymentsController(PaymentDbContext db)
    {
        _db = db;
    }

    [HttpGet]
    public async Task<IActionResult> GetPayments(
        CancellationToken cancellationToken)
    {
        var payments = await _db.Payments
            .AsNoTracking()
            .OrderByDescending(p => p.CreatedAtUtc)
            .ToListAsync(cancellationToken);

        return Ok(payments);
    }

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetPayment(
        Guid id,
        CancellationToken cancellationToken)
    {
        var payment = await _db.Payments
            .AsNoTracking()
            .FirstOrDefaultAsync(
                p => p.Id == id,
                cancellationToken);

        return payment is null
            ? NotFound()
            : Ok(payment);
    }
}