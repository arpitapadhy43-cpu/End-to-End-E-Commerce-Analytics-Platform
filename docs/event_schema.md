# Event Schema Documentation

This document defines the event schemas used in the **Event-Driven E-Commerce Data Platform**. These events are derived from the Online Retail II dataset and are published to Kafka to simulate real-time transactional activity.  

## Source Dataset

**Dataset:** Online Retail II (UK-based online retail transactions)
**Time Range:** Dec 2009 – Dec 2011

### Original Columns

| Column Name | Description                                        |
| ----------- | -------------------------------------------------- |
| Invoice     | Invoice number (starts with 'C' for cancellations) |
| StockCode   | Product identifier                                 |
| Description | Product description                                |
| Quantity    | Quantity purchased (negative for cancellations)    |
| InvoiceDate | Date and time of transaction                       |
| Price       | Unit price (GBP)                                   |
| Customer ID | Unique customer identifier                         |
| Country     | Customer country                                   |

---

## Event Design Principles

* Events represent **business actions**, not raw rows
* All events are **append-only**
* Event time is derived from `InvoiceDate`
* Schema is **forward-compatible** (new fields can be added)

---

## Event Type 1: `order_item_created`

### Description

Emitted when a product item is successfully added to an order (one event per invoice line item).

### Kafka Topic

`retail_order_events`

### Schema

```json
{
  "event_type": "order_item_created",
  "invoice_no": "string",
  "product_id": "string",
  "product_description": "string",
  "quantity": "integer",
  "unit_price": "decimal",
  "total_amount": "decimal",
  "customer_id": "string",
  "country": "string",
  "event_ts": "timestamp"
}
```

### Field Mapping

| Event Field         | Source Column    |
| ------------------- | ---------------- |
| invoice_no          | Invoice          |
| product_id          | StockCode        |
| product_description | Description      |
| quantity            | Quantity         |
| unit_price          | Price            |
| total_amount        | Quantity * Price |
| customer_id         | Customer ID      |
| country             | Country          |
| event_ts            | InvoiceDate      |

---

## Event Type 2: `order_cancelled`

### Description

Emitted when an order or order item is cancelled. Identified by invoice numbers starting with 'C' or negative quantities.

### Kafka Topic

`retail_order_events`

### Schema

```json
{
  "event_type": "order_cancelled",
  "invoice_no": "string",
  "original_invoice_no": "string",
  "product_id": "string",
  "cancelled_quantity": "integer",
  "unit_price": "decimal",
  "customer_id": "string",
  "country": "string",
  "event_ts": "timestamp"
}
```

### Field Mapping

| Event Field         | Source Column                 |
| ------------------- | ----------------------------- |
| invoice_no          | Invoice                       |
| original_invoice_no | Derived (Invoice without 'C') |
| product_id          | StockCode                     |
| cancelled_quantity  | Quantity                      |
| unit_price          | Price                         |
| customer_id         | Customer ID                   |
| country             | Country                       |
| event_ts            | InvoiceDate                   |

---

## Event Type 3: `customer_activity`

### Description

Emitted when a customer is observed for the first time in the dataset. Used to build customer dimension tables and activity metrics.

### Kafka Topic

`retail_customer_events`

### Schema

```json
{
  "event_type": "customer_activity",
  "customer_id": "string",
  "country": "string",
  "first_seen_ts": "timestamp"
}
```

### Field Mapping

| Event Field   | Source Column                     |
| ------------- | --------------------------------- |
| customer_id   | Customer ID                       |
| country       | Country                           |
| first_seen_ts | First InvoiceDate per Customer ID |

---

## Notes & Assumptions

* Events are produced in near-real-time by replaying historical data with controlled delays
* All timestamps are treated as **event time**, not processing time
* Currency is assumed to be GBP
* Invalid or corrupt records are routed to Bronze for auditing

---

## Downstream Usage

* **Bronze Layer:** Raw JSON events as ingested from Kafka
* **Silver Layer:** Schema-enforced, deduplicated events
* **Gold Layer:** Aggregated business metrics
* **Warehouse:** Star schema facts and dimensions

---

This schema will evolve as new business requirements emerge, following backward-compatible versioning principles.
