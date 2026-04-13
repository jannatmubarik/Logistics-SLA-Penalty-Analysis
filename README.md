# Logistics-SLA-Penalty-Analysis
Logistics SLA &amp; Penalty Optimization Analysis using SQL and Tableau. Built analytical models to detect shipment delays, classify SLA violations, and identify outliers using window functions and percentile logic. Applied Pareto analysis to uncover key routes and vendors contributing to 80% of penalty costs (~$923K).

## Overview

This project analyzes **shipment performance, SLA breaches, and penalty costs** across routes and vendors.
The goal is to identify:

* High-risk routes
* Outlier shipments
* Major contributors to penalty costs (Pareto 80/20)
* Persistent underperformers over time

Built using **advanced SQL (CTEs, window functions, percentiles)** and designed to power **Tableau dashboards**.

---

## Business Problem

Logistics operations often struggle with:

* Late deliveries and SLA violations
* High penalty costs without clear root causes
* Difficulty identifying which routes/vendors drive the most risk

This project answers:

> **“Where should the business focus to reduce penalties and improve SLA performance?”**

---

## Data Model

### Core Tables

#### `orders`

* `order_id`
* `route_id`
* `vendor_id`
* `order_ts`
* `promised_delivery_ts`
* `sla_hours`
* `penalty_rate_per_hour`

#### `shipments`

* `shipment_id`
* `order_id`
* `delivered_ts`
* `status`
* `actual_transit_hours`
* `late_hours`
* `penalty_cost`

---

## Key Transformations

### 1. Enriched Shipment Layer

Standardized calculation:

* `late_hours`
* `penalty_cost`

---

### 2. Outlier Detection

* Route-level **P90 / P95 thresholds**
* Shipment flagged if:

  ```
  late_hours > route P90
  ```

---

### 3. Risk Classification (Routes)

Routes labeled as:

* 🔴 HIGH_RISK
* 🟡 MEDIUM_RISK
* 🟢 LOW_RISK

Based on:

* Breach rate vs median
* Shipment volume vs median

---

### 4. Persistent Underperformance

Tracks routes appearing in **Top-K penalty ranks across months**

---

### 5. SLA Severity Segmentation

Shipments categorized into:

* Minor
* Moderate
* Major

Using:

```
relative_lateness = late_hours / sla_hours
```

---

### 6. Pareto Analysis (80/20 Rule)

#### Routes

* Top ~**80 routes contribute ~80% of total penalty (~$923K)**

#### Vendors

* Top vendors contribute majority of penalty cost
* Long tail of low-impact vendors

---

## Key Insights

### Penalty Concentration

* Total penalty: **~$923,867**
* Small subset of routes drives majority of cost

---

### High-Risk Routes

Examples:

* Route 494 → $50K+
* Route 365 → $41K+
* Route 659 → $37K+

👉 High volume + high breach = biggest risk

---

### Outliers Drive Cost

* Extreme delays (>100 hours) exist
* These disproportionately increase penalties

---

### Most Routes Are NOT Persistent Issues

* Few routes consistently appear in top penalty ranks
* Most issues are **sporadic, not systemic**

---

### Severity Distribution

* Majority = **Moderate delays**
* Small but critical segment = **Major violations (> SLA)**

---
