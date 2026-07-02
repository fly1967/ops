# ops
Order Processing System

# Running the Microservices Demo

## Prerequisites

- .NET 8 SDK (or later)
- SQL Server
- Docker Desktop
- RabbitMQ Management UI
- Visual Studio 2022 (or VS Code)

---

## 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-folder>
```

---

## 2. Start Docker Desktop

Verify Docker is running.

```bash
docker ps
```

---

## 3. Start RabbitMQ

If RabbitMQ has not been created:

```bash
docker run -d ^
  --hostname rabbit ^
  --name rabbitmq ^
  -p 5672:5672 ^
  -p 15672:15672 ^
  rabbitmq:3-management
```

If the container already exists:

```bash
docker start rabbitmq
```

Verify RabbitMQ is running:

```bash
docker ps
```

You should see something similar to:

```
rabbitmq:3-management
```

---

## 4. Verify RabbitMQ Management UI

Open:

```
http://localhost:15672
```

Login:

```
Username: guest
Password: guest
```

---

## 5. Create the Databases

Create the following SQL Server databases:

- OrdersDb
- PaymentServiceDb

Apply the Entity Framework migrations:

```bash
cd src\Services\OrderService
dotnet ef database update

cd ..\PaymentService
dotnet ef database update
```

---

## 6. Start OrderService

```bash
cd src\Services\OrderService

dotnet run
```

Verify Swagger:

```
https://localhost:<port>/swagger
```

---

## 7. Start PaymentService

Open a second terminal.

```bash
cd src\Services\PaymentService

dotnet run
```

Expected console output:

```
RabbitMqConsumer starting...
Exchange declared.
Queue declared.
Queue bound to exchange.
PaymentService is listening on payment.queue
```

---

## 8. Verify RabbitMQ Queue

Open RabbitMQ Management.

Navigate to:

```
Queues and Streams
```

Verify:

- payment.queue exists
- Consumers = 1

---

## 9. Create an Order

Using OrderService Swagger:

```
POST /api/orders
```

Create a new order.

Expected flow:

1. Order saved to OrdersDb
2. OutboxMessage created
3. OutboxPublisher publishes OrderCreated event
4. RabbitMQ routes the message
5. PaymentService receives the message
6. Payment saved to PaymentServiceDb
7. Message acknowledged

---

## Current Architecture

- OrderService
- Orders Database
- Outbox Pattern
- RabbitMQ Topic Exchange
- PaymentService
- Payment Database
- Background Services
- Correlation IDs
- Idempotent Payment Processing

---

## Current Project Status

✅ OrderService

✅ Outbox Pattern

✅ RabbitMQ Publisher

✅ RabbitMQ Consumer

✅ PaymentService

🔄 Next Phase

- InventoryService
- Inventory Reservation
- NotificationService
- Saga Pattern
- Docker Compose
- Distributed Tracing
- Health Checks
