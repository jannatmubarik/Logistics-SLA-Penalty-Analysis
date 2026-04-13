-- 3. SLA classification and aggregations
-----------------------------------------------------------------
-- 1) Per-shipment SLA classification (canonical)
-----------------------------------------------------------------
WITH shipments_enriched AS (
  SELECT
    s.shipment_id,
    s.order_id,
    o.order_ts,
    o.promised_delivery_ts,
    o.sla_hours,
    o.penalty_rate_per_hour,
    s.created_ts,
    s.pickup_ts,
    s.depart_ts,
    s.delivered_ts,
    s.status,
    s.vendor_incident_flag,
    s.demand_spike_flag,
    s.weather_severity,
    -- vendor_id comes from orders in this schema
    o.vendor_id,
    o.route_id,
    s.actual_transit_hours,
    -- promised lead time (hours)
    ROUND(TIMESTAMPDIFF(SECOND, o.order_ts, o.promised_delivery_ts) / 3600.0, 2) AS promised_lead_hours,
    -- actual lead time (hours) if delivered
    CASE
      WHEN s.delivered_ts IS NOT NULL THEN ROUND(TIMESTAMPDIFF(SECOND, o.order_ts, s.delivered_ts) / 3600.0, 2)
      ELSE NULL
    END AS actual_lead_hours,
    -- canonical late_hours: prefer existing column if present, else recompute for delivered
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL THEN ROUND(TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0, 2) ELSE NULL END
    ) AS late_hours_existing
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
),

sla_classified AS (
  SELECT
    se.*,
    -- near threshold: 15% of SLA hours or min 2 hours
    GREATEST(CEIL(COALESCE(se.sla_hours, 0) * 0.15), 2) AS near_threshold_hours,

    -- canonical late_hours column (alias) for readability
    se.late_hours_existing AS late_hours,

    -- flags
    CASE WHEN se.late_hours_existing IS NOT NULL AND se.late_hours_existing > 0 THEN 1 ELSE 0 END AS late_flag,

    CASE
      WHEN se.late_hours_existing IS NOT NULL
           AND se.late_hours_existing > 0
           AND se.late_hours_existing <= GREATEST(CEIL(COALESCE(se.sla_hours, 0) * 0.15), 2)
      THEN 1
      ELSE 0
    END AS near_breach_flag,

    -- breach boolean: we treat 'failed_non_delivery' as breach; overdue (not delivered and past promised) is breach too
    CASE
      WHEN se.status IN ('lost','damaged','returned') THEN 1
      WHEN se.late_hours_existing IS NOT NULL AND se.late_hours_existing > GREATEST(CEIL(COALESCE(se.sla_hours, 0) * 0.15), 2) THEN 1
      WHEN se.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= se.promised_delivery_ts THEN 1
      ELSE 0
    END AS is_breach,

    -- breach category priority
    CASE
      WHEN se.status IN ('lost','damaged','returned') THEN 'failed_non_delivery'

      WHEN se.delivered_ts IS NOT NULL THEN
        CASE
          WHEN ROUND(TIMESTAMPDIFF(SECOND, se.promised_delivery_ts, se.delivered_ts) / 3600.0, 2) <= 0 THEN 'on_time'
          WHEN ROUND(TIMESTAMPDIFF(SECOND, se.promised_delivery_ts, se.delivered_ts) / 3600.0, 2) <= GREATEST(CEIL(COALESCE(se.sla_hours,0) * 0.15), 2) THEN 'near_breach'
          ELSE 'breached'
        END

      WHEN se.delivered_ts IS NULL AND CURRENT_TIMESTAMP < se.promised_delivery_ts THEN 'in_flight'
      WHEN se.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= se.promised_delivery_ts THEN 'overdue'
      ELSE 'unknown'
    END AS breach_category,

    -- penalty cost calculation with fallbacks:
    CASE
      WHEN se.status IN ('lost','damaged','returned') THEN
        -- treat failed non-delivery as SLA-hours * penalty_rate (policy choice)
        COALESCE(se.penalty_rate_per_hour * se.sla_hours, 0.0)

      WHEN se.delivered_ts IS NOT NULL AND se.late_hours_existing IS NOT NULL AND se.late_hours_existing > 0 THEN
        -- delivered late: late_hours * penalty_rate_per_hour
        COALESCE(se.late_hours_existing, 0.0) * COALESCE(se.penalty_rate_per_hour, 0.0)

      WHEN se.delivered_ts IS NOT NULL AND (se.late_hours_existing IS NULL OR se.late_hours_existing <= 0) THEN
        -- delivered on time -> no penalty
        0.0

      WHEN se.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= se.promised_delivery_ts THEN
        -- overdue but not delivered: estimate penalty so far as elapsed overdue hours * rate (optional),
        -- here we compute (CURRENT_TS - promised_delivery_ts) in hours * penalty_rate_per_hour
        (TIMESTAMPDIFF(SECOND, se.promised_delivery_ts, CURRENT_TIMESTAMP) / 3600.0) * COALESCE(se.penalty_rate_per_hour, 0.0)

      ELSE 0.0
    END AS penalty_cost_calc

  FROM shipments_enriched se
)

-- Select sample rows to inspect classification
SELECT
  shipment_id,
  order_id,
  vendor_id,
  route_id,
  sla_hours,
  promised_lead_hours,
  actual_lead_hours,
  late_hours,
  late_flag,
  near_threshold_hours,
  near_breach_flag,
  is_breach,
  breach_category,
  ROUND(penalty_cost_calc, 2) AS penalty_cost_calc
FROM sla_classified
ORDER BY is_breach DESC, penalty_cost_calc DESC
LIMIT 50;



-----------------------------------------------------------------
-- 2) Aggregate breach_rate by Vendor, Route, Service Level, Month
-----------------------------------------------------------------

-- 2A: Breach rate by Vendor
WITH sla AS (  -- reuse the sla_classified CTE defined above
  SELECT * FROM (
    -- inline reuse of classification to keep one script; in production, reference vw_sla_classification
    WITH shipments_enriched AS (
      SELECT
        s.shipment_id, s.order_id, o.order_ts, o.promised_delivery_ts, o.sla_hours,
        o.penalty_rate_per_hour, s.delivered_ts, s.status, o.vendor_id, o.route_id, s.late_hours
      FROM shipments s
      JOIN orders o ON s.order_id = o.order_id
    )
    SELECT
      se.*,
      COALESCE(se.late_hours,
        CASE WHEN se.delivered_ts IS NOT NULL THEN ROUND(TIMESTAMPDIFF(SECOND, se.promised_delivery_ts, se.delivered_ts) / 3600.0, 2) ELSE NULL END
      ) AS late_hours_calc,
      GREATEST(CEIL(COALESCE(se.sla_hours, 0) * 0.15), 2) AS near_threshold_hours
    FROM shipments_enriched se
  ) t
)

SELECT
  vendor_id,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS shipments,
  SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) AS breaches,
  ROUND( (SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*),0), 3) AS breach_rate_pct,
  SUM(
    CASE
      WHEN status IN ('lost','damaged','returned') THEN COALESCE(penalty_rate_per_hour * sla_hours, 0)
      WHEN delivered_ts IS NOT NULL AND (COALESCE(late_hours_calc,0) > 0) THEN COALESCE(late_hours_calc,0) * COALESCE(penalty_rate_per_hour, 0)
      WHEN delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts THEN (TIMESTAMPDIFF(SECOND, promised_delivery_ts, CURRENT_TIMESTAMP) / 3600.0) * COALESCE(penalty_rate_per_hour,0)
      ELSE 0
    END
  ) AS total_penalty
FROM sla
GROUP BY vendor_id, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, total_penalty DESC
LIMIT 200;


-- 2B: Breach rate by Route
WITH sla AS (
  -- reuse the same inline staging as above
  SELECT
    s.shipment_id, s.order_id, o.order_ts, o.promised_delivery_ts, o.sla_hours,
    o.penalty_rate_per_hour, s.delivered_ts, s.status, o.route_id,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL THEN ROUND(TIMESTAMPDIFF(SECOND, s.delivered_ts, o.promised_delivery_ts) / 3600.0, 2) ELSE NULL END
    ) AS late_hours_calc
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
)

SELECT
  route_id,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS shipments,
  SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) AS breaches,
  ROUND( (SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*),0), 3) AS breach_rate_pct,
  SUM(
    CASE
      WHEN status IN ('lost','damaged','returned') THEN COALESCE(penalty_rate_per_hour * sla_hours, 0)
      WHEN delivered_ts IS NOT NULL AND (COALESCE(late_hours_calc,0) > 0) THEN COALESCE(late_hours_calc,0) * COALESCE(penalty_rate_per_hour, 0)
      WHEN delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts THEN (TIMESTAMPDIFF(SECOND, promised_delivery_ts, CURRENT_TIMESTAMP) / 3600.0) * COALESCE(penalty_rate_per_hour,0)
      ELSE 0
    END
  ) AS total_penalty
FROM sla
GROUP BY route_id, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, total_penalty DESC
LIMIT 200;


-- 2C: Breach rate by Service Level
WITH sla AS (
  SELECT
    s.shipment_id, s.order_id, o.order_ts, o.promised_delivery_ts, o.service_level, o.sla_hours, o.penalty_rate_per_hour,
    s.delivered_ts, s.status,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL THEN ROUND(TIMESTAMPDIFF(SECOND, s.delivered_ts, o.promised_delivery_ts) / 3600.0, 2) ELSE NULL END
    ) AS late_hours_calc
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
)

SELECT
  service_level,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS shipments,
  SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) AS breaches,
  ROUND( (SUM(CASE WHEN (COALESCE(late_hours_calc, 0) > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts) THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*),0), 3) AS breach_rate_pct,
  SUM(
    CASE
      WHEN status IN ('lost','damaged','returned') THEN COALESCE(penalty_rate_per_hour * sla_hours, 0)
      WHEN delivered_ts IS NOT NULL AND (COALESCE(late_hours_calc,0) > 0) THEN COALESCE(late_hours_calc,0) * COALESCE(penalty_rate_per_hour, 0)
      WHEN delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts THEN (TIMESTAMPDIFF(SECOND, promised_delivery_ts, CURRENT_TIMESTAMP) / 3600.0) * COALESCE(penalty_rate_per_hour,0)
      ELSE 0
    END
  ) AS total_penalty
FROM sla
GROUP BY service_level, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, total_penalty DESC
LIMIT 200;


-----------------------------------------------------------------
-- 3) Avg promised lead time vs actual lead time (by Vendor / Route / ServiceLevel / Month)
-----------------------------------------------------------------

WITH lead_times AS (
  SELECT
    o.order_id,
    o.vendor_id,
    o.route_id,
    o.service_level,
    o.order_ts,
    o.promised_delivery_ts,
    s.delivered_ts,
    -- promised lead time in hours
    TIMESTAMPDIFF(SECOND, o.order_ts, o.promised_delivery_ts) / 3600.0 AS promised_lead_hours,
    -- actual lead time in hours (NULL if not delivered)
    CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.order_ts, s.delivered_ts) / 3600.0 ELSE NULL END AS actual_lead_hours
  FROM orders o
  LEFT JOIN shipments s ON s.order_id = o.order_id
)

-- 3A: by Vendor + month
SELECT
  vendor_id,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS orders_count,
  ROUND(AVG(promised_lead_hours), 2) AS avg_promised_lead_hours,
  ROUND(AVG(actual_lead_hours), 2) AS avg_actual_lead_hours,
  ROUND( (AVG(actual_lead_hours) - AVG(promised_lead_hours)), 2 ) AS avg_actual_minus_promised_hours
FROM lead_times
GROUP BY vendor_id, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, avg_actual_minus_promised_hours DESC
LIMIT 200;


-- 3B: by Route + month
WITH lead_times AS (
  SELECT
    o.order_id,
    o.vendor_id,
    o.route_id,
    o.service_level,
    o.order_ts,
    o.promised_delivery_ts,
    s.delivered_ts,
    -- promised lead time in hours
    TIMESTAMPDIFF(SECOND, o.order_ts, o.promised_delivery_ts) / 3600.0 AS promised_lead_hours,
    -- actual lead time in hours (NULL if not delivered)
    CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.order_ts, s.delivered_ts) / 3600.0 ELSE NULL END AS actual_lead_hours
  FROM orders o
  LEFT JOIN shipments s ON s.order_id = o.order_id
)

SELECT
  route_id,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS orders_count,
  ROUND(AVG(promised_lead_hours), 2) AS avg_promised_lead_hours,
  ROUND(AVG(actual_lead_hours), 2) AS avg_actual_lead_hours,
  ROUND( (AVG(actual_lead_hours) - AVG(promised_lead_hours)), 2 ) AS avg_actual_minus_promised_hours
FROM lead_times
GROUP BY route_id, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, avg_actual_minus_promised_hours DESC
LIMIT 200;


-- 3C: by Service Level + month
WITH lead_times AS (
  SELECT
    o.order_id,
    o.vendor_id,
    o.route_id,
    o.service_level,
    o.order_ts,
    o.promised_delivery_ts,
    s.delivered_ts,
    -- promised lead time in hours
    TIMESTAMPDIFF(SECOND, o.order_ts, o.promised_delivery_ts) / 3600.0 AS promised_lead_hours,
    -- actual lead time in hours (NULL if not delivered)
    CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.order_ts, s.delivered_ts) / 3600.0 ELSE NULL END AS actual_lead_hours
  FROM orders o
  LEFT JOIN shipments s ON s.order_id = o.order_id
)

SELECT
  service_level,
  DATE(DATE_FORMAT(order_ts, '%Y-%m-01')) AS month_start,
  COUNT(*) AS orders_count,
  ROUND(AVG(promised_lead_hours), 2) AS avg_promised_lead_hours,
  ROUND(AVG(actual_lead_hours), 2) AS avg_actual_lead_hours,
  ROUND( (AVG(actual_lead_hours) - AVG(promised_lead_hours)), 2 ) AS avg_actual_minus_promised_hours
FROM lead_times
GROUP BY service_level, DATE(DATE_FORMAT(order_ts, '%Y-%m-01'))
ORDER BY month_start DESC, avg_actual_minus_promised_hours DESC
LIMIT 200;


-- 4. Root Causes Derivation

-- 4A) dominant-cause per shipment (terminate with semicolon)
WITH enriched AS (
  -- (same enriched CTE as before)
  SELECT
    s.shipment_id,
    s.order_id,
    o.order_ts,
    o.promised_delivery_ts,
    s.created_ts,
    s.pickup_ts,
    s.depart_ts,
    s.delivered_ts,
    s.status,
    s.vendor_incident_flag,
    s.demand_spike_flag,
    s.weather_severity,
    s.weather_bucket,
    s.actual_transit_hours,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
        THEN ROUND(TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0, 2)
        ELSE NULL
      END
    ) AS late_hours_calc,
    COALESCE(s.penalty_cost, 0.0) AS penalty_cost,
    o.vendor_id,
    v.vendor_tier,
    v.base_ontime_rate,
    v.capacity_index,
    o.route_id,
    r.origin_region,
    r.dest_region,
    r.congestion_index,
    r.route_risk_score,
    r.mode,
    r.distance_band,
    o.service_level,
    o.sla_hours,
    o.penalty_rate_per_hour,
    s.breach_reason_true
  FROM shipments s
  JOIN orders o       ON s.order_id = o.order_id
  LEFT JOIN vendors v ON o.vendor_id = v.vendor_id
  LEFT JOIN routes r  ON o.route_id = r.route_id
),
scored AS (
  SELECT
    *,
    (CAST(vendor_incident_flag AS SIGNED) * 5.0) AS vendor_w,
    (COALESCE(weather_severity, 0.0) * 3.0) AS weather_w,
    (CASE
       WHEN demand_spike_flag = 1 AND COALESCE(capacity_index, 1.0) != 0
         THEN (10.0 / NULLIF(capacity_index, 0.0)) * COALESCE(congestion_index, 1.0)
       ELSE 0.0
     END) AS congestion_w,
    (GREATEST(COALESCE(route_risk_score, 0.0), 0.0) * 1.5) AS risk_w
  FROM enriched
),
dominant AS (
  SELECT
    shipment_id,
    order_id,
    order_ts,
    promised_delivery_ts,
    created_ts,
    pickup_ts,
    depart_ts,
    delivered_ts,
    status,
    vendor_id,
    vendor_tier,
    base_ontime_rate,
    capacity_index,
    route_id,
    origin_region,
    dest_region,
    mode,
    distance_band,
    sla_hours,
    penalty_rate_per_hour,
    late_hours_calc AS late_hours,
    penalty_cost,
    vendor_incident_flag,
    demand_spike_flag,
    weather_severity,
    weather_bucket,
    actual_transit_hours,
    vendor_w, weather_w, congestion_w, risk_w,
    breach_reason_true,
    GREATEST(vendor_w, weather_w, congestion_w, risk_w) AS max_score,
    CASE
      WHEN GREATEST(vendor_w, weather_w, congestion_w, risk_w) = 0 THEN 'unknown'
      WHEN vendor_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'vendor_incident'
      WHEN weather_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'weather'
      WHEN congestion_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'route_congestion'
      WHEN risk_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'route_risk'
      ELSE 'unknown'
    END AS dominant_cause
  FROM scored
)
SELECT
  shipment_id,
  order_id,
  vendor_id,
  route_id,
  late_hours,
  penalty_cost,
  vendor_incident_flag,
  demand_spike_flag,
  weather_severity,
  vendor_w, weather_w, congestion_w, risk_w,
  dominant_cause,
  breach_reason_true
FROM dominant
;  -- <- required terminator to run a new statement next

-- 4B) top routes (separate statement)
WITH per_route AS (
  SELECT
    o.route_id,
    COUNT(*) AS shipments,
    SUM(COALESCE(s.penalty_cost, (COALESCE(s.late_hours,0) * o.penalty_rate_per_hour))) AS total_penalty
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.route_id
),
ranked AS (
  SELECT
    route_id,
    shipments,
    total_penalty,
    SUM(total_penalty) OVER (ORDER BY total_penalty DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    SUM(total_penalty) OVER () AS total_penalty_all
  FROM per_route
  WHERE total_penalty IS NOT NULL
),
pct AS (
  SELECT
    route_id,
    shipments,
    total_penalty,
    running_total,
    total_penalty_all,
    CASE
      WHEN total_penalty_all = 0 THEN 0.0
      ELSE CAST(running_total AS DECIMAL(24,6)) / CAST(total_penalty_all AS DECIMAL(24,6))
    END AS cumulative_pct,
    LAG(
      CASE
        WHEN total_penalty_all = 0 THEN 0.0
        ELSE CAST(running_total AS DECIMAL(24,6)) / CAST(total_penalty_all AS DECIMAL(24,6))
      END
    ) OVER (ORDER BY running_total) AS prev_cumulative_pct
  FROM ranked
)
SELECT
  route_id,
  shipments,
  total_penalty,
  running_total,
  total_penalty_all,
  cumulative_pct,
  prev_cumulative_pct
FROM pct
ORDER BY running_total DESC
;

-- 4C. Derived breach flag and canonical breach selection (MariaDB)
WITH base AS (
  SELECT
    s.shipment_id,
    s.order_id,
    COALESCE(
      s.late_hours,
      CASE
        WHEN s.delivered_ts IS NOT NULL
          THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
        ELSE NULL
      END
    ) AS late_hours,
    s.status,
    s.delivered_ts,
    o.promised_delivery_ts,
    -- treat non-delivery as severe breach
    CASE
      WHEN s.status IN ('lost','damaged','returned') THEN 1
      WHEN s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts THEN 1
      WHEN COALESCE(s.late_hours, 0) > 0 THEN 1
      ELSE 0
    END AS is_breach_flag,
    -- classify "type" where possible (delivered-late vs non-delivered)
    CASE
      WHEN s.status IN ('lost','damaged','returned') THEN 'failed_non_delivery'
      WHEN s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts THEN 'overdue_inflight'
      WHEN COALESCE(s.late_hours, 0) > 0 THEN 'delivered_late'
      ELSE 'not_breach'
    END AS derived_breach_type
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
)
SELECT *
FROM base
WHERE is_breach_flag = 1;

-- 4D -- Root cause distribution among breaches (MariaDB)
WITH enriched AS (
  SELECT
    s.shipment_id,
    s.order_id,
    s.status,
    s.delivered_ts,
    o.promised_delivery_ts,
    COALESCE(
      s.late_hours,
      CASE
        WHEN s.delivered_ts IS NOT NULL
          THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
        ELSE NULL
      END
    ) AS late_hours,
    s.vendor_incident_flag,
    s.demand_spike_flag,
    s.weather_severity,
    v.capacity_index,
    r.congestion_index,
    r.route_risk_score
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  LEFT JOIN vendors v ON o.vendor_id = v.vendor_id
  LEFT JOIN routes r ON o.route_id = r.route_id
),

scored AS (
  SELECT
    *,
    (CAST(vendor_incident_flag AS SIGNED) * 5.0) AS vendor_w,
    (COALESCE(weather_severity, 0.0) * 3.0) AS weather_w,
    (CASE
       WHEN demand_spike_flag = 1 AND COALESCE(capacity_index,1.0) != 0
         THEN (10.0 / NULLIF(capacity_index,0.0)) * COALESCE(congestion_index,1.0)
       ELSE 0.0
     END) AS congestion_w,
    (GREATEST(COALESCE(route_risk_score,0.0),0.0) * 1.5) AS risk_w
  FROM enriched
),

dominant AS (
  SELECT
    *,
    CASE
      WHEN GREATEST(vendor_w, weather_w, congestion_w, risk_w) = 0 THEN 'unknown'
      WHEN vendor_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'vendor_incident'
      WHEN weather_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'weather'
      WHEN congestion_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'route_congestion'
      WHEN risk_w = GREATEST(vendor_w, weather_w, congestion_w, risk_w) THEN 'route_risk'
      ELSE 'unknown'
    END AS dominant_cause
  FROM scored
),

breaches AS (
  SELECT *
  FROM dominant
  WHERE
      late_hours > 0
      OR status IN ('lost','damaged','returned')
      OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts)
),

counts AS (
  SELECT
    dominant_cause,
    COUNT(*) AS breach_count
  FROM breaches
  GROUP BY dominant_cause
)

SELECT
  dominant_cause,
  breach_count,
  ROUND(
    (breach_count / NULLIF(SUM(breach_count) OVER (),0)) * 100,
    2
  ) AS pct_of_all_breaches
FROM counts
ORDER BY breach_count DESC;


-- 5A. Route-level statistics
WITH
-- base enrichment (compute late_hours where missing, bring in route/region)
enriched AS (
  SELECT
    s.shipment_id,
    o.order_ts,
    o.promised_delivery_ts,
    s.delivered_ts,
    s.status,
    COALESCE(s.late_hours,
      CASE
        WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
        ELSE NULL
      END
    ) AS late_hours,
    COALESCE(s.penalty_cost, (COALESCE(s.late_hours, 0) * COALESCE(o.penalty_rate_per_hour, 0))) AS penalty_cost,
    o.route_id,
    r.origin_region,
    r.dest_region,
    r.baseline_transit_hours,
    s.actual_transit_hours
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  LEFT JOIN routes r ON o.route_id = r.route_id
),

-- only delivered shipments for percentile calculations
route_delivered AS (
  SELECT *
  FROM enriched
  WHERE status = 'delivered'
),

/* Build ranks per route ordered by actual_transit_hours so we can extract empirical percentiles
   (This avoids relying on PERCENTILE_CONT which isn't available in many MariaDB installs.)
*/
transit_ranks AS (
  SELECT
    rd.route_id,
    rd.actual_transit_hours,
    ROW_NUMBER() OVER (PARTITION BY rd.route_id ORDER BY rd.actual_transit_hours) AS rn,
    COUNT(*) OVER (PARTITION BY rd.route_id) AS cnt
  FROM route_delivered rd
),

-- For each route, compute the p90/p95 by taking the first value whose rank >= ceil(pct*cnt)
route_percentiles AS (
  SELECT
    tr.route_id,
    -- when cnt = 0 these will be NULL
    MIN(CASE WHEN tr.rn >= CEIL(0.90 * tr.cnt) THEN tr.actual_transit_hours END) AS p90_transit_hours,
    MIN(CASE WHEN tr.rn >= CEIL(0.95 * tr.cnt) THEN tr.actual_transit_hours END) AS p95_transit_hours,
    AVG(tr.actual_transit_hours) AS avg_transit_hours_delivered
  FROM transit_ranks tr
  GROUP BY tr.route_id
),

-- Aggregate counts / breach counts / penalties over all shipments (not limited to delivered)
route_agg AS (
  SELECT
    e.route_id,
    e.origin_region,
    e.dest_region,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (e.late_hours > 0)
          OR e.status IN ('lost','damaged','returned')
          OR (e.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= e.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    ROUND(
      CASE
        WHEN COUNT(*) = 0 THEN NULL
        ELSE 100.0 * SUM(
          CASE
            WHEN (e.late_hours > 0)
              OR e.status IN ('lost','damaged','returned')
              OR (e.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= e.promised_delivery_ts)
            THEN 1 ELSE 0
          END
        ) / COUNT(*)
      END
    , 2) AS breach_rate_pct,
    ROUND(AVG(e.actual_transit_hours), 2) AS avg_transit_hours_all,
    SUM(e.penalty_cost) AS total_penalty
  FROM enriched e
  GROUP BY e.route_id, e.origin_region, e.dest_region
)

SELECT
  ra.route_id,
  ra.origin_region,
  ra.dest_region,
  ra.shipments,
  ra.breach_count,
  ra.breach_rate_pct,
  rp.p90_transit_hours,
  rp.p95_transit_hours,
  -- prefer the delivered-only avg if available, else fall back to avg across all
  COALESCE(ROUND(rp.avg_transit_hours_delivered,2), ra.avg_transit_hours_all) AS avg_transit_hours,
  ra.total_penalty
FROM route_agg ra
LEFT JOIN route_percentiles rp ON rp.route_id = ra.route_id
-- ORDER: put NULLs last for the percentile/breach sorts by using boolean expressions
ORDER BY
  (ra.breach_rate_pct IS NULL), ra.breach_rate_pct DESC,
  (rp.p95_transit_hours IS NULL), rp.p95_transit_hours DESC;

-- 5B. Vendor Level Stats

WITH
enriched AS (
  SELECT
    s.shipment_id,
    o.order_ts,
    o.promised_delivery_ts,
    s.delivered_ts,
    s.status,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
        THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
        ELSE NULL
      END
    ) AS late_hours,
    COALESCE(s.penalty_cost, (COALESCE(s.late_hours, 0) * COALESCE(o.penalty_rate_per_hour, 0))) AS penalty_cost,
    o.vendor_id,
    v.vendor_tier,
    v.capacity_index,
    s.actual_transit_hours,
    s.vendor_incident_flag,
    s.demand_spike_flag
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  LEFT JOIN vendors v ON o.vendor_id = v.vendor_id
),

-- limit to delivered shipments for percentile calculations (empirical percentiles)
vendor_delivered AS (
  SELECT *
  FROM enriched
  WHERE status = 'delivered'
),

-- rank delivered shipments per vendor by transit time
transit_ranks AS (
  SELECT
    vd.vendor_id,
    vd.actual_transit_hours,
    ROW_NUMBER() OVER (PARTITION BY vd.vendor_id ORDER BY vd.actual_transit_hours) AS rn,
    COUNT(*) OVER (PARTITION BY vd.vendor_id) AS cnt
  FROM vendor_delivered vd
),

-- compute p90/p95 per vendor using the empirical step-percentile (first value with rn >= ceil(pct*cnt))
vendor_percentiles AS (
  SELECT
    tr.vendor_id,
    MIN(CASE WHEN tr.rn >= CEIL(0.90 * tr.cnt) THEN tr.actual_transit_hours END) AS p90_transit_hours,
    MIN(CASE WHEN tr.rn >= CEIL(0.95 * tr.cnt) THEN tr.actual_transit_hours END) AS p95_transit_hours,
    AVG(tr.actual_transit_hours) AS avg_transit_hours_delivered
  FROM transit_ranks tr
  GROUP BY tr.vendor_id
),

vendor_agg AS (
  SELECT
    e.vendor_id,
    e.vendor_tier,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (e.late_hours > 0)
          OR e.status IN ('lost','damaged','returned')
          OR (e.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= e.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    ROUND(
      CASE WHEN COUNT(*) = 0 THEN NULL
      ELSE 100.0 * SUM(
        CASE
          WHEN (e.late_hours > 0)
            OR e.status IN ('lost','damaged','returned')
            OR (e.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= e.promised_delivery_ts)
          THEN 1 ELSE 0
        END
      ) / COUNT(*)
      END
    , 2) AS breach_rate_pct,
    SUM(e.penalty_cost) AS total_penalty,
    -- average late hours among breaches (NULLs ignored)
    ROUND(AVG(CASE WHEN e.late_hours > 0 THEN e.late_hours ELSE NULL END), 2) AS avg_late_hours_among_breaches,
    -- fallback avg across all shipments (kept for comparison)
    ROUND(AVG(e.actual_transit_hours), 2) AS avg_transit_hours_all,
    SUM(CASE WHEN COALESCE(e.vendor_incident_flag,0) = 1 THEN 1 ELSE 0 END) AS vendor_incident_count,
    ROUND(
      CASE WHEN COUNT(*) = 0 THEN NULL
      ELSE 100.0 * SUM(CASE WHEN COALESCE(e.vendor_incident_flag,0) = 1 THEN 1 ELSE 0 END) / COUNT(*)
      END
    , 2) AS vendor_incident_rate_pct,
    AVG(e.capacity_index) AS avg_capacity_index
  FROM enriched e
  GROUP BY e.vendor_id, e.vendor_tier
)

SELECT
  va.vendor_id,
  va.vendor_tier,
  va.shipments,
  va.breach_count,
  va.breach_rate_pct,
  va.total_penalty,
  va.avg_late_hours_among_breaches,
  vp.p90_transit_hours,
  vp.p95_transit_hours,
  -- prefer the delivered-only avg if present, else fall back
  COALESCE(ROUND(vp.avg_transit_hours_delivered, 2), va.avg_transit_hours_all) AS avg_transit_hours,
  va.vendor_incident_count,
  va.vendor_incident_rate_pct,
  va.avg_capacity_index
FROM vendor_agg va
LEFT JOIN vendor_percentiles vp ON vp.vendor_id = va.vendor_id
ORDER BY (va.total_penalty IS NULL), va.total_penalty DESC;

-- 5C. Rolling Metrics (60 Day and 90 Day)
-- -- 5C (i) Overall daily rolling breach rate (last 60 days shown)
WITH daily AS (
  SELECT
    DATE(o.order_ts) AS day,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
             COALESCE(s.late_hours,
               CASE WHEN s.delivered_ts IS NOT NULL
                 THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
               END
             ) > 0
           )
          OR s.status IN ('lost','damaged','returned')
          OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY DATE(o.order_ts)
)

SELECT
  day,
  shipments,
  breaches,
  ROUND((breaches * 1.0 / NULLIF(shipments,0)) * 100, 3) AS breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      ORDER BY day
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_7d_breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_30d_breach_rate_pct
FROM daily
ORDER BY day DESC
LIMIT 60;

-- -- 5C (ii) Vendor-level rolling breach rate (last 90 days)
WITH daily_vendor AS (
  SELECT
    o.vendor_id,
    DATE(o.order_ts) AS day,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
             COALESCE(s.late_hours,
               CASE WHEN s.delivered_ts IS NOT NULL
                 THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
               END
             ) > 0
           )
          OR s.status IN ('lost','damaged','returned')
          OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id, DATE(o.order_ts)
)

SELECT
  vendor_id,
  day,
  shipments,
  breaches,
  ROUND((breaches * 1.0 / NULLIF(shipments,0)) * 100, 3) AS daily_breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      PARTITION BY vendor_id
      ORDER BY day
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_7d_breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      PARTITION BY vendor_id
      ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_30d_breach_rate_pct
FROM daily_vendor
WHERE day >= CURRENT_DATE - INTERVAL 90 DAY
ORDER BY vendor_id, day DESC;

-- -- 5C (iii) Route-level rolling breach rate (last 90 days)
WITH daily_route AS (
  SELECT
    o.route_id,
    DATE(o.order_ts) AS day,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
             COALESCE(s.late_hours,
               CASE WHEN s.delivered_ts IS NOT NULL
                 THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
               END
             ) > 0
           )
          OR s.status IN ('lost','damaged','returned')
          OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.route_id, DATE(o.order_ts)
)

SELECT
  route_id,
  day,
  shipments,
  breaches,
  ROUND((breaches * 1.0 / NULLIF(shipments,0)) * 100, 3) AS daily_breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      PARTITION BY route_id
      ORDER BY day
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_7d_breach_rate_pct,
  ROUND(
    AVG(breaches * 1.0 / NULLIF(shipments,1)) OVER (
      PARTITION BY route_id
      ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * 100
  , 3) AS rolling_30d_breach_rate_pct
FROM daily_route
WHERE day >= CURRENT_DATE - INTERVAL 90 DAY
ORDER BY route_id, day DESC;

-- 5D. Vendor monthly KPIs with rank
WITH base AS (
  SELECT
    o.vendor_id,
    -- first day of month as DATE
    DATE(DATE_FORMAT(o.order_ts, '%Y-%m-01')) AS month_start,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
               COALESCE(
                 s.late_hours,
                 CASE
                   WHEN s.delivered_ts IS NOT NULL
                   THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
                   ELSE NULL
                 END
               ) > 0
             )
          OR s.status IN ('lost','damaged','returned')
          OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    SUM(
      COALESCE(
        s.penalty_cost,
        (COALESCE(s.late_hours,
          CASE WHEN s.delivered_ts IS NOT NULL
            THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
          ELSE NULL END
        ) * COALESCE(o.penalty_rate_per_hour, 0.0))
      , 0.0)
    ) AS total_penalty,
    SUM(
      COALESCE(
        s.late_hours,
        CASE
          WHEN s.delivered_ts IS NOT NULL
          THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
          ELSE 0
        END
      )
    ) AS sum_late_hours
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id, DATE(DATE_FORMAT(o.order_ts, '%Y-%m-01'))
),

metrics AS (
  SELECT
    vendor_id,
    month_start,
    shipments,
    breach_count,
    -- percent
    ROUND( (breach_count * 1.0 / NULLIF(shipments, 0)) * 100.0, 3) AS breach_rate_pct,
    total_penalty,
    -- penalty per 1,000 orders (total_penalty / shipments * 1000)
    ROUND( (total_penalty * 1000.0 / NULLIF(shipments, 0)), 2) AS penalty_per_1k_orders,
    -- average late hours among breaches; NULL when breach_count = 0
    CASE
      WHEN breach_count = 0 THEN NULL
      ELSE ROUND( (sum_late_hours * 1.0 / breach_count), 2)
    END AS avg_late_hours_among_breaches
  FROM base
)

SELECT
  vendor_id,
  month_start,
  shipments,
  breach_count,
  breach_rate_pct,
  total_penalty,
  penalty_per_1k_orders,
  avg_late_hours_among_breaches,
  RANK() OVER (PARTITION BY month_start ORDER BY total_penalty DESC) AS penalty_rank_in_month,
  RANK() OVER (PARTITION BY month_start ORDER BY breach_rate_pct DESC) AS breach_rate_rank_in_month
FROM metrics
ORDER BY month_start DESC, penalty_rank_in_month;

-- 5E. Frequency vs Severity: who drives the penalty more?
-- -- 5E (i) VENDOR — Frequency vs Severity (normalized) + composite risk score (A)
-- Vendor-level frequency vs severity (min-max normalization + composite score)
WITH v AS (
  SELECT
    o.vendor_id,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
          COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL
              THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
            END
          ) > 0
        )
        OR s.status IN ('lost','damaged','returned')
        OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    SUM(COALESCE(s.penalty_cost,
         (COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL
              THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
            END
         ) * COALESCE(o.penalty_rate_per_hour, 0.0))
    , 0.0)) AS total_penalty,
    AVG(CASE WHEN COALESCE(s.late_hours,0) > 0 THEN COALESCE(s.late_hours,0) ELSE NULL END) AS avg_late_hours_among_breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id
),

stats AS (
  SELECT
    vendor_id,
    shipments,
    breach_count,
    (breach_count * 1.0 / NULLIF(shipments, 0)) AS breach_rate,
    total_penalty,
    -- replace NULL avg_late_hours with 0 for normalization convenience
    COALESCE(avg_late_hours_among_breaches, 0.0) AS avg_late_hours_among_breaches
  FROM v
),

normalized AS (
  SELECT
    vendor_id,
    shipments,
    breach_count,
    breach_rate,
    total_penalty,
    avg_late_hours_among_breaches,
    -- min-max normalization: handle zero range -> produce 0
    CASE
      WHEN (MAX(breach_rate) OVER() - MIN(breach_rate) OVER()) = 0 THEN 0.0
      ELSE (breach_rate - MIN(breach_rate) OVER()) / (MAX(breach_rate) OVER() - MIN(breach_rate) OVER())
    END AS freq_norm,
    CASE
      WHEN (MAX(avg_late_hours_among_breaches) OVER() - MIN(avg_late_hours_among_breaches) OVER()) = 0 THEN 0.0
      ELSE (avg_late_hours_among_breaches - MIN(avg_late_hours_among_breaches) OVER()) / (MAX(avg_late_hours_among_breaches) OVER() - MIN(avg_late_hours_among_breaches) OVER())
    END AS sev_norm
  FROM stats
)

SELECT
  vendor_id,
  shipments,
  breach_count,
  ROUND(breach_rate * 100.0, 3) AS breach_rate_pct,
  total_penalty,
  ROUND(avg_late_hours_among_breaches, 3) AS avg_late_hours_among_breaches,
  ROUND(freq_norm, 3) AS freq_norm,
  ROUND(sev_norm, 3) AS sev_norm,
  -- tunable composite (0.6 freq, 0.4 severity in this example)
  ROUND((freq_norm * 0.6 + sev_norm * 0.4), 3) AS composite_risk_score
FROM normalized
ORDER BY total_penalty DESC;

-- -- 5E (ii) ROUTE — Frequency vs Severity (same logic)
-- Route-level frequency vs severity
WITH r AS (
  SELECT
    o.route_id,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
          COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL
              THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
            END
          ) > 0
        )
        OR s.status IN ('lost','damaged','returned')
        OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    SUM(COALESCE(s.penalty_cost,
         (COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL
              THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
            END
         ) * COALESCE(o.penalty_rate_per_hour, 0.0))
    , 0.0)) AS total_penalty,
    AVG(CASE WHEN COALESCE(s.late_hours,0) > 0 THEN COALESCE(s.late_hours,0) ELSE NULL END) AS avg_late_hours_among_breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.route_id
),

stats AS (
  SELECT
    route_id,
    shipments,
    breach_count,
    (breach_count * 1.0 / NULLIF(shipments, 0)) AS breach_rate,
    total_penalty,
    COALESCE(avg_late_hours_among_breaches, 0.0) AS avg_late_hours_among_breaches
  FROM r
),

normalized AS (
  SELECT
    route_id,
    shipments,
    breach_count,
    breach_rate,
    total_penalty,
    avg_late_hours_among_breaches,
    CASE
      WHEN (MAX(breach_rate) OVER() - MIN(breach_rate) OVER()) = 0 THEN 0.0
      ELSE (breach_rate - MIN(breach_rate) OVER()) / (MAX(breach_rate) OVER() - MIN(breach_rate) OVER())
    END AS freq_norm,
    CASE
      WHEN (MAX(avg_late_hours_among_breaches) OVER() - MIN(avg_late_hours_among_breaches) OVER()) = 0 THEN 0.0
      ELSE (avg_late_hours_among_breaches - MIN(avg_late_hours_among_breaches) OVER()) / (MAX(avg_late_hours_among_breaches) OVER() - MIN(avg_late_hours_among_breaches) OVER())
    END AS sev_norm
  FROM stats
)

SELECT
  route_id,
  shipments,
  breach_count,
  ROUND(breach_rate * 100.0, 3) AS breach_rate_pct,
  total_penalty,
  ROUND(avg_late_hours_among_breaches, 3) AS avg_late_hours_among_breaches,
  ROUND(freq_norm, 3) AS freq_norm,
  ROUND(sev_norm, 3) AS sev_norm,
  ROUND((freq_norm * 0.6 + sev_norm * 0.4), 3) AS composite_risk_score
FROM normalized
ORDER BY total_penalty DESC;

-- -- 5E (iii) CORRELATION between frequency and severity (Pearson) — vendor example (B)
-- Pearson correlation between breach_rate and avg_late_hours_among_breaches (vendor-level)
WITH v AS (
  SELECT
    o.vendor_id,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
          COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL
              THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
            END
          ) > 0
        )
        OR s.status IN ('lost','damaged','returned')
        OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    AVG(CASE WHEN COALESCE(s.late_hours,0) > 0 THEN COALESCE(s.late_hours,0) ELSE NULL END) AS avg_late_hours_among_breaches
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id
),

agg AS (
  SELECT
    vendor_id,
    (breach_count * 1.0 / NULLIF(shipments, 0)) AS breach_rate,
    COALESCE(avg_late_hours_among_breaches, 0.0) AS avg_late_hours_among_breaches
  FROM v
  WHERE shipments > 0
),

stats AS (
  SELECT
    COUNT(*) AS n,
    AVG(breach_rate) AS mean_breach_rate,
    AVG(avg_late_hours_among_breaches) AS mean_sev
  FROM agg
)

SELECT
  -- basic counts
  s.n AS vendor_count,
  -- means
  ROUND(s.mean_breach_rate * 100.0, 4) AS mean_breach_rate_pct,
  ROUND(s.mean_sev, 4) AS mean_avg_late_hours,
  -- covariance (population)
  ROUND( SUM( (a.breach_rate - s.mean_breach_rate) * (a.avg_late_hours_among_breaches - s.mean_sev) ) / s.n, 6) AS covariance_pop,
  -- population stddevs
  ROUND( STDDEV_POP(a.breach_rate), 6) AS stddev_breach_rate_pop,
  ROUND( STDDEV_POP(a.avg_late_hours_among_breaches), 6) AS stddev_sev_pop,
  -- Pearson correlation = cov / (std_x * std_y)
  CASE
    WHEN STDDEV_POP(a.breach_rate) = 0 OR STDDEV_POP(a.avg_late_hours_among_breaches) = 0 THEN NULL
    ELSE ROUND(
      ( SUM( (a.breach_rate - s.mean_breach_rate) * (a.avg_late_hours_among_breaches - s.mean_sev) ) / s.n )
      / ( STDDEV_POP(a.breach_rate) * STDDEV_POP(a.avg_late_hours_among_breaches) )
    , 6)
  END AS pearson_corr_breach_vs_severity
FROM agg a
CROSS JOIN stats s;

-- 5F. Vendor Risk Profile (High incident flag, High late severity)
WITH base AS (
  SELECT
    o.vendor_id,
    COUNT(*) AS shipments,
    SUM(CASE WHEN COALESCE(s.vendor_incident_flag, 0) = 1 THEN 1 ELSE 0 END) AS vendor_incident_count,
    SUM(
      CASE
        WHEN (
          COALESCE(
            s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0 ELSE NULL END
          ) > 0
        )
        OR s.status IN ('lost','damaged','returned')
        OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
        THEN 1 ELSE 0
      END
    ) AS breach_count,
    SUM(
      COALESCE(
        s.penalty_cost,
        (COALESCE(s.late_hours,
           CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0 ELSE 0 END
        ) * COALESCE(o.penalty_rate_per_hour, 0.0))
      , 0.0)
    ) AS total_penalty,
    AVG(CASE WHEN COALESCE(s.late_hours, 0) > 0 THEN COALESCE(s.late_hours, 0) ELSE NULL END) AS avg_late_hours_raw
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id
),

-- core metrics (normalize and explicit numeric math)
metrics AS (
  SELECT
    vendor_id,
    shipments,
    vendor_incident_count,
    breach_count,
    -- percentage of shipments flagged as vendor incidents
    (vendor_incident_count * 100.0 / NULLIF(shipments, 0)) AS incident_rate_pct,
    -- breach rate percent
    (breach_count * 100.0 / NULLIF(shipments, 0)) AS breach_rate_pct,
    total_penalty,
    -- treat NULL avg_late_hours as 0.0 for downstream median/comparison; change if you want to exclude
    COALESCE(avg_late_hours_raw, 0.0) AS avg_late_hours
  FROM base
),

/* median for incident_rate_pct:
   - compute row numbers and total count, then take the middle 1 or 2 rows and avg them (works for even/odd n)
*/
incident_order AS (
  SELECT
    vendor_id,
    incident_rate_pct,
    ROW_NUMBER() OVER (ORDER BY incident_rate_pct) AS rn,
    COUNT(*) OVER () AS cnt
  FROM metrics
),

median_incident AS (
  SELECT AVG(incident_rate_pct) AS median_incident_rate
  FROM incident_order
  WHERE rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2))
),

/* median for avg_late_hours (uses avg_late_hours computed above) */
late_order AS (
  SELECT
    vendor_id,
    avg_late_hours,
    ROW_NUMBER() OVER (ORDER BY avg_late_hours) AS rn,
    COUNT(*) OVER () AS cnt
  FROM metrics
),

median_late AS (
  SELECT AVG(avg_late_hours) AS median_avg_late_hours
  FROM late_order
  WHERE rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2))
),

thresholds AS (
  SELECT
    mi.median_incident_rate,
    ml.median_avg_late_hours
  FROM median_incident mi
  CROSS JOIN median_late ml
),

profile AS (
  SELECT
    m.*,
    t.median_incident_rate,
    t.median_avg_late_hours,
    CASE
      WHEN m.incident_rate_pct >= (t.median_incident_rate * 1.5)
        OR m.avg_late_hours >= (t.median_avg_late_hours * 1.5)
      THEN 'High Risk'
      WHEN m.incident_rate_pct >= (t.median_incident_rate * 1.0)
        OR m.avg_late_hours >= (t.median_avg_late_hours * 1.0)
      THEN 'Medium Risk'
      ELSE 'Low Risk'
    END AS risk_label
  FROM metrics m
  CROSS JOIN thresholds t
)

SELECT
  vendor_id,
  shipments,
  vendor_incident_count,
  ROUND(incident_rate_pct, 3) AS incident_rate_pct,
  breach_count,
  ROUND(breach_rate_pct, 3) AS breach_rate_pct,
  ROUND(avg_late_hours, 3) AS avg_late_hours_among_breaches,
  total_penalty,
  ROUND(median_incident_rate, 3) AS median_incident_rate,
  ROUND(median_avg_late_hours, 3) AS median_avg_late_hours,
  risk_label
FROM profile
ORDER BY
  CASE risk_label WHEN 'High Risk' THEN 3 WHEN 'Medium Risk' THEN 2 ELSE 1 END DESC,
  total_penalty DESC;


-- 6. Outlier flags
-- 6A. P90 threshold per route (approximate)
WITH enriched AS (
  SELECT
    o.route_id,
    s.shipment_id,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
           ELSE NULL END) AS late_hours,
    s.actual_transit_hours,
    s.status
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
),
late_ranks AS (
  SELECT
    route_id,
    late_hours,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY late_hours) AS rn,
    COUNT(*) OVER (PARTITION BY route_id) AS cnt
  FROM enriched
  WHERE late_hours IS NOT NULL
),
transit_ranks AS (
  SELECT
    route_id,
    actual_transit_hours,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY actual_transit_hours) AS rn,
    COUNT(*) OVER (PARTITION BY route_id) AS cnt
  FROM enriched
  WHERE actual_transit_hours IS NOT NULL
),
route_p90_late AS (
  SELECT
    lr.route_id,
    MIN(CASE WHEN lr.rn >= CEIL(0.90 * lr.cnt) THEN lr.late_hours END) AS p90_late_hours,
    MIN(CASE WHEN lr.rn >= CEIL(0.95 * lr.cnt) THEN lr.late_hours END) AS p95_late_hours
  FROM late_ranks lr
  GROUP BY lr.route_id
),
route_p90_transit AS (
  SELECT
    tr.route_id,
    MIN(CASE WHEN tr.rn >= CEIL(0.90 * tr.cnt) THEN tr.actual_transit_hours END) AS p90_transit_hours
  FROM transit_ranks tr
  GROUP BY tr.route_id
)

SELECT
  e.route_id,
  COUNT(*) AS shipments_total,
  SUM(CASE WHEN e.late_hours IS NOT NULL THEN 1 ELSE 0 END) AS delivered_with_late_hours,
  rpl.p90_late_hours,
  rpl.p95_late_hours,
  rpt.p90_transit_hours
FROM enriched e
LEFT JOIN route_p90_late rpl ON e.route_id = rpl.route_id
LEFT JOIN route_p90_transit rpt ON e.route_id = rpt.route_id
WHERE e.late_hours IS NOT NULL
GROUP BY e.route_id, rpl.p90_late_hours, rpl.p95_late_hours, rpt.p90_transit_hours
ORDER BY rpl.p90_late_hours DESC;


-- 6B. Flag shipments in top 10% per route (shipment-level outlier flag)
WITH
late_vals AS (
  SELECT
    o.route_id,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
           ELSE NULL END) AS late_hours
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
),
late_ranks AS (
  SELECT
    route_id,
    late_hours,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY late_hours) AS rn,
    COUNT(*) OVER (PARTITION BY route_id) AS cnt
  FROM late_vals
  WHERE late_hours IS NOT NULL
),
route_p90 AS (
  SELECT
    lr.route_id,
    MIN(CASE WHEN lr.rn >= CEIL(0.90 * lr.cnt) THEN lr.late_hours END) AS p90_late_hours
  FROM late_ranks lr
  GROUP BY lr.route_id
),
shipments_with_late AS (
  SELECT
    s.shipment_id,
    o.route_id,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
           ELSE NULL END) AS late_hours,
    s.status,
    s.penalty_cost
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
)

SELECT
  swl.*,
  rp.p90_late_hours,
  CASE
    WHEN swl.late_hours IS NULL THEN NULL
    WHEN rp.p90_late_hours IS NULL THEN NULL
    WHEN swl.late_hours > rp.p90_late_hours THEN 1
    ELSE 0
  END AS is_route_p90_outlier
FROM shipments_with_late swl
LEFT JOIN route_p90 rp ON swl.route_id = rp.route_id
ORDER BY is_route_p90_outlier DESC, late_hours DESC;


-- 6C. High Risk Routes (High breach % AND High volume)
WITH base AS (
  SELECT
    o.route_id,
    COUNT(*) AS shipments,
    SUM(
      CASE
        WHEN (
          COALESCE(s.late_hours,
            CASE WHEN s.delivered_ts IS NOT NULL THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0 END
          ) > 0
        )
        OR s.status IN ('lost','damaged','returned')
        OR (s.delivered_ts IS NULL AND CURRENT_TIMESTAMP >= o.promised_delivery_ts)
      THEN 1 ELSE 0 END
    ) AS breach_count,
    SUM(
      COALESCE(s.penalty_cost,
        (COALESCE(s.late_hours,0) * COALESCE(o.penalty_rate_per_hour,0.0))
      )
    ) AS total_penalty
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.route_id
),
metrics AS (
  SELECT
    route_id,
    shipments,
    breach_count,
    (breach_count * 100.0 / NULLIF(shipments,0)) AS breach_rate_pct,
    total_penalty
  FROM base
),
order_breach AS (
  SELECT
    route_id,
    breach_rate_pct,
    ROW_NUMBER() OVER (ORDER BY breach_rate_pct) AS rn,
    COUNT(*) OVER () AS cnt
  FROM metrics
),
median_breach AS (
  SELECT AVG(breach_rate_pct) AS median_breach_rate
  FROM order_breach
  WHERE rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2))
),
order_shipments AS (
  SELECT
    route_id,
    shipments,
    ROW_NUMBER() OVER (ORDER BY shipments) AS rn,
    COUNT(*) OVER () AS cnt
  FROM metrics
),
median_shipments AS (
  SELECT AVG(shipments) AS median_shipments
  FROM order_shipments
  WHERE rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2))
),
thresholds AS (
  SELECT mb.median_breach_rate, ms.median_shipments
  FROM median_breach mb CROSS JOIN median_shipments ms
)

SELECT
  m.*,
  t.median_breach_rate,
  t.median_shipments,
  CASE
    WHEN m.breach_rate_pct >= (t.median_breach_rate * 1.5) AND m.shipments >= (t.median_shipments * 1.25)
      THEN 'HIGH_RISK'
    WHEN m.breach_rate_pct >= (t.median_breach_rate * 1.2) OR m.shipments >= (t.median_shipments * 1.1)
      THEN 'MEDIUM_RISK'
    ELSE 'LOW_RISK'
  END AS route_risk_label
FROM metrics m
CROSS JOIN thresholds t
ORDER BY route_risk_label DESC, breach_rate_pct DESC;

-- 6D. Persistent Underperformers (Routes & Vendors repeatedly in top penalty ranks)
WITH RECURSIVE
params AS (
  SELECT 6 AS last_n_months, 10 AS top_k
),
months AS (
  SELECT DATE_SUB(DATE_FORMAT(CURRENT_DATE, '%Y-%m-01'), INTERVAL (last_n_months - 1) MONTH) AS month_start
  FROM params
  UNION ALL
  SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
  FROM months, params
  WHERE month_start < DATE_FORMAT(CURRENT_DATE, '%Y-%m-01')
),
route_monthly AS (
  SELECT
    DATE(DATE_FORMAT(o.order_ts, '%Y-%m-01')) AS month_start,
    o.route_id,
    SUM(
      COALESCE(s.penalty_cost, (COALESCE(s.late_hours,0) * COALESCE(o.penalty_rate_per_hour,0.0)))
    ) AS total_penalty
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  WHERE DATE(DATE_FORMAT(o.order_ts, '%Y-%m-01')) >=
        DATE_SUB(DATE_FORMAT(CURRENT_DATE, '%Y-%m-01'),
                 INTERVAL ((SELECT last_n_months FROM params) - 1) MONTH)
  GROUP BY DATE(DATE_FORMAT(o.order_ts, '%Y-%m-01')), o.route_id
),
ranked AS (
  SELECT
    rm.*,
    RANK() OVER (PARTITION BY rm.month_start ORDER BY rm.total_penalty DESC) AS penalty_rank_in_month
  FROM route_monthly rm
),
topk AS (
  SELECT
    month_start,
    route_id,
    total_penalty,
    penalty_rank_in_month
  FROM ranked
  WHERE penalty_rank_in_month <= (SELECT top_k FROM params)
),
summary AS (
  SELECT
    r.route_id,
    COUNT(DISTINCT r.month_start) AS months_in_window,
    SUM(CASE WHEN t.route_id IS NOT NULL THEN 1 ELSE 0 END) AS months_in_topk,
    ROUND(
      SUM(CASE WHEN t.route_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 /
      NULLIF(COUNT(DISTINCT r.month_start),0),
    2) AS pct_months_in_topk,
    SUM(CASE WHEN t.total_penalty IS NOT NULL THEN t.total_penalty ELSE 0 END) AS penalty_while_topk
  FROM route_monthly r
  LEFT JOIN topk t
    ON r.route_id = t.route_id
   AND r.month_start = t.month_start
  GROUP BY r.route_id
)

SELECT
  s.*,
  CASE WHEN s.months_in_topk >= 3 THEN 'PERSISTENT_UNDERPERFORMER' ELSE 'TRANSIENT' END AS underperformer_label
FROM summary s
ORDER BY months_in_topk DESC, penalty_while_topk DESC;


-- 6E. SLA Violation Severity Segments (Minor / Moderate / Major)
WITH base AS (
  SELECT
    s.shipment_id,
    o.order_id,
    o.sla_hours,
    s.delivered_ts,
    COALESCE(s.late_hours,
      CASE WHEN s.delivered_ts IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND, o.promised_delivery_ts, s.delivered_ts) / 3600.0
           ELSE NULL END) AS late_hours,
    s.status,
    COALESCE(s.penalty_cost, (COALESCE(s.late_hours,0) * COALESCE(o.penalty_rate_per_hour,0.0))) AS penalty_cost,
    o.promised_delivery_ts
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
),
breaches AS (
  SELECT *
  FROM base
  WHERE (late_hours > 0) OR status IN ('lost','damaged','returned') OR (delivered_ts IS NULL AND CURRENT_TIMESTAMP >= promised_delivery_ts)
),
segmented AS (
  SELECT
    shipment_id,
    order_id,
    sla_hours,
    late_hours,
    penalty_cost,
    CASE WHEN sla_hours IS NULL OR sla_hours = 0 THEN NULL ELSE (late_hours / NULLIF(sla_hours,0)) END AS relative_lateness,
    CASE
      WHEN late_hours IS NULL THEN 'unknown'
      WHEN ( (sla_hours IS NOT NULL AND sla_hours > 0 AND (late_hours / sla_hours) <= 0.2) OR late_hours <= 4 ) THEN 'Minor'
      WHEN ( (sla_hours IS NOT NULL AND sla_hours > 0 AND (late_hours / sla_hours) <= 1.0) OR (late_hours > 4 AND late_hours <= 24) ) THEN 'Moderate'
      WHEN ( (sla_hours IS NOT NULL AND sla_hours > 0 AND (late_hours / sla_hours) > 1.0) OR late_hours > 24 ) THEN 'Major'
      ELSE 'unknown'
    END AS severity_label
  FROM breaches
)
SELECT
  shipment_id,
  order_id,
  sla_hours,
  late_hours,
  ROUND(COALESCE(relative_lateness,0), 3) AS relative_lateness,
  severity_label,
  penalty_cost
FROM segmented
ORDER BY severity_label DESC, late_hours DESC;





-- 7. High-impact” routes & vendors: Top routes contributing to 80% of penalty cost, Same for vendors
-- Top Routes: smallest set of routes that cumulatively contribute >= 80% of total penalty
WITH per_route AS (
  SELECT
    o.route_id,
    COUNT(*) AS shipments,
    SUM(COALESCE(s.penalty_cost, (COALESCE(s.late_hours,0) * o.penalty_rate_per_hour))) AS total_penalty
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.route_id
),

ranked AS (
  SELECT
    route_id,
    shipments,
    total_penalty,
    SUM(total_penalty) OVER (ORDER BY total_penalty DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    SUM(total_penalty) OVER () AS total_penalty_all
  FROM per_route
  WHERE total_penalty IS NOT NULL
),

pct AS (
  SELECT
    route_id,
    shipments,
    total_penalty,
    running_total,
    total_penalty_all,
    CASE
      WHEN total_penalty_all = 0 THEN 0.0
      ELSE CAST(running_total AS DECIMAL(24,6)) / CAST(total_penalty_all AS DECIMAL(24,6))
    END AS cumulative_pct,
    LAG(
      CASE
        WHEN total_penalty_all = 0 THEN 0.0
        ELSE CAST(running_total AS DECIMAL(24,6)) / CAST(total_penalty_all AS DECIMAL(24,6))
      END
    ) OVER (ORDER BY running_total) AS prev_cumulative_pct
  FROM ranked
)

SELECT
  route_id,
  shipments,
  total_penalty,
  running_total,
  total_penalty_all,
  ROUND(cumulative_pct * 100.0, 3) AS cumulative_pct_pct,
  CASE
    WHEN cumulative_pct <= 0.80 THEN 1
    WHEN COALESCE(prev_cumulative_pct, 0.0) < 0.80 AND cumulative_pct >= 0.80 THEN 1
    ELSE 0
  END AS in_top_80
FROM pct
ORDER BY total_penalty DESC;

 
-- Top Vendors: smallest set of vendors that cumulatively contribute >= 80% of total penalty
WITH per_vendor AS (
  SELECT
    o.vendor_id,
    COUNT(*) AS shipments,
    SUM(COALESCE(s.penalty_cost, (COALESCE(s.late_hours,0) * o.penalty_rate_per_hour))) AS total_penalty
  FROM shipments s
  JOIN orders o ON s.order_id = o.order_id
  GROUP BY o.vendor_id
),

ranked AS (
  SELECT
    vendor_id,
    shipments,
    total_penalty,
    SUM(total_penalty) OVER (ORDER BY total_penalty DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    SUM(total_penalty) OVER () AS total_penalty_all
  FROM per_vendor
  WHERE total_penalty IS NOT NULL
),

-- compute cumulative_pct, then compute prev_cumulative_pct in next CTE
pct_base AS (
  SELECT
    vendor_id,
    shipments,
    total_penalty,
    running_total,
    total_penalty_all,
    CASE
      WHEN total_penalty_all = 0 THEN 0.0
      ELSE CAST(running_total AS DECIMAL(24,6)) / CAST(total_penalty_all AS DECIMAL(24,6))
    END AS cumulative_pct
  FROM ranked
),

pct_with_prev AS (
  SELECT
    p.*,
    LAG(p.cumulative_pct) OVER (ORDER BY p.running_total) AS prev_cumulative_pct
  FROM pct_base p
)

SELECT
  vendor_id,
  shipments,
  total_penalty,
  running_total,
  total_penalty_all,
  ROUND(cumulative_pct * 100.0, 3) AS cumulative_pct_pct,
  CASE
    WHEN cumulative_pct <= 0.80 THEN 1
    WHEN COALESCE(prev_cumulative_pct, 0.0) < 0.80 AND cumulative_pct >= 0.80 THEN 1
    ELSE 0
  END AS in_top_80
FROM pct_with_prev
ORDER BY total_penalty DESC;
