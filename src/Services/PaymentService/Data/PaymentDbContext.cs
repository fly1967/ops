using Microsoft.EntityFrameworkCore;
using PaymentService.Entities;

namespace PaymentService.Data;

public sealed class PaymentDbContext : DbContext
{
    public PaymentDbContext(DbContextOptions<PaymentDbContext> options)
        : base(options)
    {
    }

    public DbSet<Payment> Payments => Set<Payment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Payment>(entity =>
        {
            entity.HasKey(p => p.Id);

            entity.HasIndex(p => p.OrderId)
                .IsUnique();
            
            entity.Property(p => p.Amount)
                .HasColumnType("decimal(18,2)");

            entity.Property(p => p.Status)
                .HasMaxLength(50)
                .IsRequired();

            entity.Property(p => p.CreatedAtUtc)
                .IsRequired();
        });
    }
}