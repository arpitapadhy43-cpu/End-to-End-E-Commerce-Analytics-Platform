# Event Schema Documentation

This document defines the event schemas used in the **Event-Driven E-Commerce Analytics Platform**. Events are derived from the Online Retail II dataset and published to Kafka to simulate real-time transactional activity.

---

## Source Dataset

**Dataset:** Online Retail II (UK-based online retail transactions)  
**Time Range:** Dec 2009 – Dec 2010  
**Total Records:** 525,461 line items

### Original Columns

| Column Name | Description |
|---|---|
| Invoice | Invoice number |
| StockCode | Product identifier |
| Description | Product description |
| Quantity | Quantity purchased (negative = cancellation) |
| InvoiceDate | Date and time of transaction |
| Price | Unit price (GBP) |
| Customer ID | Unique customer identifier (nullable — guest orders) |
| Country | Customer country |

---

## Event Design Principles

- Events represent **business actions**, not raw rows
- All events are **append-only**
- Event time is derived from `InvoiceDate`
- Schema is **forward-compatible** — new fields can be added without breaking consumers
- `NaN` values are serialised as JSON `null` (not Python `float('nan')`)

---

## Event Type 1: `order_item_created`

### Description
Emitted for each line item on an order where `Quantity > 0`. One event per invoice line item.

### Kafka Topic
`retail_order_events`

### Schema

```json
{
  "event_type": "order_item_created",
  "invoice_id": "string",
  "stock_code": "string",
  "description": "string | null",
  "quantity": "integer",
  "unit_price": "decimal",
  "total_amount": "decimal",
  "customer_id": "integer | null",
  "country": "string",
  "event_time": "timestamp",
  "ingestion_time": "timestamp"
}
```

### Field Mapping

| Event Field | Source Column | Notes |
|---|---|---|
| invoice_id | Invoice | Cast to string |
| stock_code | StockCode | Cast to string |
| description | Description | Null if missing |
| quantity | Quantity | Positive integer |
| unit_price | Price | Float |
| total_amount | Quantity * Price | Computed |
| customer_id | Customer ID | Null for guest orders |
| country | Country | String |
| event_time | InvoiceDate | ISO timestamp string |
| ingestion_time | — | Producer wall clock time |

---

## Event Type 2: `order_item_cancelled`

### Description
Emitted for each line item where `Quantity < 0`. Quantity is stored as its absolute value in the event.

### Kafka Topic
`retail_order_events`

### Schema

```json
{
  "event_type": "order_item_cancelled",
  "invoice_id": "string",
  "stock_code": "string",
  "description": "string | null",
  "quantity": "integer",
  "unit_price": "decimal",
  "total_amount": "decimal",
  "customer_id": "integer | null",
  "country": "string",
  "event_time": "timestamp",
  "ingestion_time": "timestamp"
}
```

### Field Mapping

| Event Field | Source Column | Notes |
|---|---|---|
| invoice_id | Invoice | Cast to string |
| stock_code | StockCode | Cast to string |
| description | Description | Null if missing |
| quantity | abs(Quantity) | Stored as positive integer |
| unit_price | Price | Float |
| total_amount | abs(Quantity * Price) | Computed, always positive |
| customer_id | Customer ID | Null for guest orders |
| country | Country | String |
| event_time | InvoiceDate | ISO timestamp string |
| ingestion_time | — | Producer wall clock time |

---

## Event Type 3: `customer_activity`

### Description
Emitted once per unique customer — the first time a `Customer ID` is observed in the dataset. Used to build the customer dimension table.

### Kafka Topic
`retail_customer_events`

### Schema

```json
{
  "event_type": "customer_activity",
  "customer_id": "integer",
  "country": "string",
  "first_seen_time": "timestamp",
  "ingestion_time": "timestamp"
}
```

### Field Mapping

| Event Field | Source Column | Notes |
|---|---|---|
| customer_id | Customer ID | Integer, never null |
| country | Country | String |
| first_seen_time | InvoiceDate | First occurrence per customer |
| ingestion_time | — | Producer wall clock time |

---

## Known Data Quality Issues

| Issue | Description | Resolution |
|---|---|---|
| NaN description | 2,928 rows have null `description`, causing `json.dumps` to emit literal `NaN` (invalid JSON) | Fixed in silver layer using `regexp_replace` before JSON parsing |
| Duplicate records | 6,979 truly duplicate records exist in bronze (identical on all 4 key fields) | Removed in silver via `dropDuplicates(["invoice_id", "stock_code", "event_time", "quantity"])` |
| Guest orders | 107,927 rows have no `Customer ID` | Retained in orders, excluded from customer dimension, bucketed as "guest" in CLV table |
| Producer checkpoint gap | 36 rows skipped due to producer restart at checkpoint boundary | Accepted loss — 0.007% of total records |

---

## Downstream Usage

| Layer | Usage |
|---|---|
| Bronze | Raw JSON envelopes stored as Parquet in MinIO — source of truth |
| Silver | Schema-enforced, typed, deduplicated events partitioned by year/month |
| Gold | Star schema facts, dimensions, and pre-aggregated reporting tables |
| PostgreSQL | Serving layer for BI dashboards |