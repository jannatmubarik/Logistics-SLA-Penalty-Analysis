-- ================================================================================
-- LOGISTICS SLA BREACH & ROOT CAUSE ANALYZER
-- DIRECT OUTPUT VERSION - No Views, Just Results
-- ================================================================================
-- Purpose: Every query below produces immediate output
-- Usage: Run each query individually to see results instantly
-- ================================================================================

-- ################################################################################
-- SECTION 1: DATA QUALITY CHECK
-- ################################################################################

-- ================================================================================
-- 1.1 ORDERS DATA QUALITY
-- ================================================================================

SELECT 
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_orders,
    MIN(order_ts) AS earliest_order,
    MAX(order_ts) AS latest_order
FROM orders;


-- ================================================================================
-- 1.2 SHIPMENTS DATA QUALITY
-- ================================================================================
SELECT 
    COUNT(*) AS total_shipments,
    COUNT(DISTINCT shipment_id) AS unique_shipments,
    COUNT(*) - COUNT(DISTINCT shipment_id) AS duplicate_shipments,
    SUM(CASE WHEN late_hours > 0 THEN 1 ELSE 0 END) AS breached_shipments,
    ROUND(SUM(CASE WHEN late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct
FROM shipments;


-- ################################################################################
-- SECTION 2: EXECUTIVE SUMMARY
-- ################################################################################

-- ================================================================================
-- 2.1 OVERALL PERFORMANCE METRICS
-- ================================================================================
SELECT '=== EXECUTIVE SUMMARY - OVERALL PERFORMANCE ===' AS section;

SELECT 
    COUNT(DISTINCT s.shipment_id) AS total_shipments,
    COUNT(DISTINCT o.vendor_id) AS active_vendors,
    COUNT(DISTINCT o.route_id) AS active_routes,
    
    -- Breach metrics
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS total_breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    
    -- Financial impact
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.penalty_cost), 2) AS avg_penalty_per_shipment,
    
    -- Performance
    ROUND(AVG(s.late_hours), 2) AS avg_late_hours,
    ROUND(AVG(CASE WHEN s.late_hours > 0 THEN s.late_hours END), 2) AS avg_late_hours_when_breach

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
WHERE s.status = 'delivered';


-- ================================================================================
-- 2.2 ROOT CAUSE BREAKDOWN
-- ================================================================================
SELECT '=== BREACH ROOT CAUSES ===' AS section;

SELECT 
    breach_reason_true AS root_cause,
    COUNT(*) AS breach_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_breaches,
    ROUND(SUM(penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(late_hours), 2) AS avg_late_hours
FROM shipments
WHERE late_hours > 0
  AND status = 'delivered'
GROUP BY breach_reason_true
ORDER BY breach_count DESC;


-- ================================================================================
-- 2.3 SERVICE LEVEL PERFORMANCE
-- ================================================================================
SELECT '=== PERFORMANCE BY SERVICE LEVEL ===' AS section;

SELECT 
    o.service_level,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.actual_transit_hours), 2) AS avg_transit_hours
FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
WHERE s.status = 'delivered'
GROUP BY o.service_level
ORDER BY breach_rate_pct DESC;


-- ################################################################################
-- SECTION 3: WEEKLY TRENDS
-- ################################################################################

-- ================================================================================
-- 3.1 LAST 8 WEEKS PERFORMANCE TREND
-- ================================================================================
SELECT '=== WEEKLY PERFORMANCE TREND (Last 8 Weeks) ===' AS section;

SELECT 
    YEAR(s.created_ts) AS year,
    WEEK(s.created_ts, 1) AS week,
    MIN(DATE(s.created_ts)) AS week_start_date,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    
    -- Breach reasons
    SUM(CASE WHEN s.breach_reason_true = 'vendor_incident' THEN 1 ELSE 0 END) AS vendor_incidents,
    SUM(CASE WHEN s.breach_reason_true = 'weather' THEN 1 ELSE 0 END) AS weather_issues,
    SUM(CASE WHEN s.breach_reason_true = 'route_congestion' THEN 1 ELSE 0 END) AS congestion_issues

FROM shipments s
WHERE s.status = 'delivered'
  AND s.created_ts >= DATE_SUB(NOW(), INTERVAL 8 WEEK)
GROUP BY YEAR(s.created_ts), WEEK(s.created_ts, 1)
ORDER BY year DESC, week DESC;


-- ################################################################################
-- SECTION 4: VENDOR PERFORMANCE
-- ################################################################################

-- ================================================================================
-- 4.1 TOP 10 WORST PERFORMING VENDORS
-- ================================================================================
SELECT '=== TOP 10 WORST PERFORMING VENDORS ===' AS section;

SELECT 
    v.vendor_id,
    v.vendor_tier,
    v.primary_hub_region,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.late_hours), 2) AS avg_late_hours,
    
    -- Performance gap
    ROUND(v.base_ontime_rate * 100, 2) AS expected_ontime_pct,
    ROUND((1 - SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100, 2) AS actual_ontime_pct

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN vendors v ON o.vendor_id = v.vendor_id
WHERE s.status = 'delivered'
GROUP BY v.vendor_id, v.vendor_tier, v.primary_hub_region, v.base_ontime_rate
HAVING COUNT(*) >= 20
ORDER BY total_penalty_cost DESC
LIMIT 10;


-- ================================================================================
-- 4.2 VENDOR TIER COMPARISON
-- ================================================================================
SELECT '=== PERFORMANCE BY VENDOR TIER ===' AS section;

SELECT 
    v.vendor_tier,
    COUNT(DISTINCT v.vendor_id) AS vendor_count,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(v.base_ontime_rate) * 100, 2) AS avg_expected_ontime_pct

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN vendors v ON o.vendor_id = v.vendor_id
WHERE s.status = 'delivered'
GROUP BY v.vendor_tier
ORDER BY FIELD(v.vendor_tier, 'gold', 'silver', 'bronze');


-- ################################################################################
-- SECTION 5: ROUTE ANALYSIS
-- ################################################################################

-- ================================================================================
-- 5.1 TOP 10 WORST PERFORMING ROUTES
-- ================================================================================
SELECT '=== TOP 10 WORST PERFORMING ROUTES ===' AS section;

SELECT 
    r.route_id,
    CONCAT(r.origin_region, ' → ', r.dest_region) AS corridor,
    r.mode AS transport_mode,
    r.distance_band,
    ROUND(r.distance_km, 2) AS distance_km,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) AS breaches,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(r.congestion_index), 3) AS avg_congestion_index

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.route_id, r.origin_region, r.dest_region, r.mode, r.distance_band, r.distance_km
HAVING COUNT(*) >= 10
ORDER BY total_penalty_cost DESC
LIMIT 10;


-- ================================================================================
-- 5.2 DISTANCE VS PERFORMANCE
-- ================================================================================
SELECT '=== PERFORMANCE BY DISTANCE BAND ===' AS section;

SELECT 
    r.distance_band,
    r.mode AS transport_mode,
    COUNT(*) AS total_shipments,
    ROUND(AVG(r.distance_km), 2) AS avg_distance_km,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.actual_transit_hours), 2) AS avg_actual_transit_hours,
    ROUND(AVG(r.baseline_transit_hours), 2) AS avg_baseline_transit_hours

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.distance_band, r.mode
ORDER BY FIELD(r.distance_band, 'short', 'medium', 'long', 'xlong'), r.mode;


-- ================================================================================
-- 5.3 REGIONAL CORRIDOR ANALYSIS (Top 15)
-- ================================================================================
SELECT '=== TOP 15 CORRIDORS BY PENALTY COST ===' AS section;

SELECT 
    CONCAT(r.origin_region, ' → ', r.dest_region) AS corridor,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(r.distance_km), 2) AS avg_distance_km,
    ROUND(AVG(r.congestion_index), 3) AS avg_congestion_index

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.origin_region, r.dest_region
HAVING COUNT(*) >= 10
ORDER BY total_penalty_cost DESC
LIMIT 15;


-- ================================================================================
-- 5.4 DOMESTIC VS INTERNATIONAL
-- ================================================================================
SELECT '=== DOMESTIC VS INTERNATIONAL SHIPMENTS ===' AS section;

SELECT 
    CASE 
        WHEN r.origin_country = r.dest_country THEN 'Domestic'
        ELSE 'International'
    END AS route_type,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.late_hours), 2) AS avg_late_hours,
    ROUND(AVG(r.distance_km), 2) AS avg_distance_km,
    
    -- Breach reason breakdown
    ROUND(SUM(CASE WHEN s.breach_reason_true = 'vendor_incident' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END), 0), 2) AS vendor_incident_pct,
    ROUND(SUM(CASE WHEN s.breach_reason_true = 'weather' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END), 0), 2) AS weather_pct

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY CASE WHEN r.origin_country = r.dest_country THEN 'Domestic' ELSE 'International' END;


-- ################################################################################
-- SECTION 6: GEOGRAPHIC HOTSPOTS
-- ################################################################################

-- ================================================================================
-- 6.1 TOP 10 ORIGIN REGIONS BY PENALTY COST
-- ================================================================================
SELECT '=== TOP 10 ORIGIN PENALTY HOTSPOTS ===' AS section;

SELECT 
    r.origin_region,
    r.origin_country,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(r.congestion_index), 3) AS avg_congestion_index

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.origin_region, r.origin_country
ORDER BY total_penalty_cost DESC
LIMIT 10;


-- ================================================================================
-- 6.2 TOP 10 DESTINATION REGIONS BY PENALTY COST
-- ================================================================================
SELECT '=== TOP 10 DESTINATION PENALTY HOTSPOTS ===' AS section;

SELECT 
    r.dest_region,
    r.dest_country,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(r.congestion_index), 3) AS avg_congestion_index

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.dest_region, r.dest_country
ORDER BY total_penalty_cost DESC
LIMIT 10;


-- ################################################################################
-- SECTION 7: WEATHER IMPACT
-- ################################################################################

-- ================================================================================
-- 7.1 BREACH RATE BY WEATHER SEVERITY
-- ================================================================================
SELECT '=== PERFORMANCE BY WEATHER SEVERITY ===' AS section;

SELECT 
    s.weather_bucket AS weather_condition,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    ROUND(AVG(s.weather_severity), 3) AS avg_weather_severity,
    ROUND(AVG(s.late_hours), 2) AS avg_late_hours,
    
    -- Weather-attributed breaches
    ROUND(SUM(CASE WHEN s.breach_reason_true = 'weather' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END), 0), 2) AS weather_attributed_pct

FROM shipments s
WHERE s.status = 'delivered'
GROUP BY s.weather_bucket
ORDER BY FIELD(s.weather_bucket, 'low', 'medium', 'high');


-- ================================================================================
-- 7.2 WEATHER × TRANSPORT MODE INTERACTION
-- ================================================================================
SELECT '=== WEATHER IMPACT BY TRANSPORT MODE ===' AS section;

SELECT 
    r.mode AS transport_mode,
    s.weather_bucket AS weather_condition,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.mode, s.weather_bucket
ORDER BY r.mode, FIELD(s.weather_bucket, 'low', 'medium', 'high');


-- ================================================================================
-- 7.3 WEATHER BY REGION
-- ================================================================================
SELECT '=== WEATHER IMPACT BY REGION (Top 10) ===' AS section;

SELECT 
    r.origin_region,
    COUNT(*) AS total_shipments,
    ROUND(AVG(s.weather_severity), 3) AS avg_weather_severity,
    SUM(CASE WHEN s.weather_bucket = 'high' THEN 1 ELSE 0 END) AS high_weather_shipments,
    ROUND(SUM(CASE WHEN s.weather_bucket = 'high' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_weather_pct,
    
    -- Breach rates by weather
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS overall_breach_rate,
    ROUND(SUM(CASE WHEN s.weather_bucket = 'high' AND s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN s.weather_bucket = 'high' THEN 1 ELSE 0 END), 0), 2) AS breach_rate_high_weather

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
INNER JOIN routes r ON o.route_id = r.route_id
WHERE s.status = 'delivered'
GROUP BY r.origin_region
ORDER BY avg_weather_severity DESC
LIMIT 10;


-- ================================================================================
-- 7.4 DEMAND SPIKE + WEATHER COMBINED
-- ================================================================================
SELECT '=== DEMAND SPIKE × WEATHER INTERACTION ===' AS section;

SELECT 
    CASE WHEN s.demand_spike_flag = 1 THEN 'Demand Spike' ELSE 'Normal Demand' END AS demand_condition,
    s.weather_bucket AS weather_condition,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_rate_pct,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    
    -- Risk level
    CASE 
        WHEN s.demand_spike_flag = 1 AND s.weather_bucket = 'high' THEN 'CRITICAL'
        WHEN s.demand_spike_flag = 1 OR s.weather_bucket = 'high' THEN 'ELEVATED'
        ELSE 'NORMAL'
    END AS combined_risk_level

FROM shipments s
WHERE s.status = 'delivered'
GROUP BY s.demand_spike_flag, s.weather_bucket
ORDER BY s.demand_spike_flag DESC, FIELD(s.weather_bucket, 'low', 'medium', 'high');


-- ################################################################################
-- SECTION 8: PREDICTIVE SIGNALS
-- ################################################################################

-- ================================================================================
-- 8.1 PICKUP DELAY AS EARLY WARNING
-- ================================================================================
SELECT '=== PICKUP DELAY → BREACH PROBABILITY ===' AS section;

SELECT 
    CASE 
        WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 2 THEN '0-2 hours'
        WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 4 THEN '2-4 hours'
        WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 8 THEN '4-8 hours'
        WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 12 THEN '8-12 hours'
        WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 24 THEN '12-24 hours'
        ELSE '24+ hours'
    END AS pickup_delay_bucket,
    COUNT(*) AS total_shipments,
    ROUND(SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS breach_probability_pct,
    ROUND(AVG(TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts)), 2) AS avg_pickup_delay_hours,
    ROUND(SUM(s.penalty_cost), 2) AS total_penalty_cost,
    
    -- Risk category
    CASE 
        WHEN SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 40 THEN 'HIGH RISK'
        WHEN SUM(CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 20 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS risk_category

FROM shipments s
INNER JOIN orders o ON s.order_id = o.order_id
WHERE s.status = 'delivered'
  AND s.pickup_ts IS NOT NULL
GROUP BY CASE 
    WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 2 THEN '0-2 hours'
    WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 4 THEN '2-4 hours'
    WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 8 THEN '4-8 hours'
    WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 12 THEN '8-12 hours'
    WHEN TIMESTAMPDIFF(HOUR, o.order_ts, s.pickup_ts) < 24 THEN '12-24 hours'
    ELSE '24+ hours'
END
ORDER BY FIELD(pickup_delay_bucket, '0-2 hours', '2-4 hours', '4-8 hours', '8-12 hours', '12-24 hours', '24+ hours');


-- ================================================================================
-- 8.2 COMPOSITE RISK SCORE VALIDATION
-- ================================================================================
SELECT '=== RISK SCORE VALIDATION ===' AS section;

WITH risk_scores AS (
    SELECT 
        s.shipment_id,
        -- Calculate composite risk score
        ROUND(
            (s.weather_severity * 30) +  -- Weather: 30 points
            (LEAST(r.congestion_index / 2 * 25, 25)) +  -- Congestion: 25 points
            ((1 - v.base_ontime_rate) * 100) +  -- Vendor: 20 points
            (CASE 
                WHEN r.distance_band = 'short' THEN 5
                WHEN r.distance_band = 'medium' THEN 10
                WHEN r.distance_band = 'long' THEN 13
                WHEN r.distance_band = 'xlong' THEN 15
                ELSE 10
            END),  -- Distance: 15 points
        2) AS risk_score,
        CASE WHEN s.late_hours > 0 THEN 1 ELSE 0 END AS actual_breach
    FROM shipments s
    INNER JOIN orders o ON s.order_id = o.order_id
    INNER JOIN vendors v ON o.vendor_id = v.vendor_id
    INNER JOIN routes r ON o.route_id = r.route_id
    WHERE s.status = 'delivered'
)
SELECT 
    CASE 
        WHEN risk_score >= 60 THEN 'CRITICAL'
        WHEN risk_score >= 40 THEN 'HIGH'
        WHEN risk_score >= 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_category,
    COUNT(*) AS total_shipments,
    SUM(actual_breach) AS actual_breaches,
    ROUND(SUM(actual_breach) * 100.0 / COUNT(*), 2) AS actual_breach_rate_pct,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    ROUND(MIN(risk_score), 2) AS min_risk_score,
    ROUND(MAX(risk_score), 2) AS max_risk_score
FROM risk_scores
GROUP BY CASE 
    WHEN risk_score >= 60 THEN 'CRITICAL'
    WHEN risk_score >= 40 THEN 'HIGH'
    WHEN risk_score >= 25 THEN 'MEDIUM'
    ELSE 'LOW'
END
ORDER BY FIELD(risk_category, 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL');


-- ################################################################################
-- END OF ANALYSIS
-- ################################################################################

SELECT '========================================' AS divider;
SELECT 'ANALYSIS COMPLETE!' AS status;
SELECT '========================================' AS divider;
SELECT 'All queries executed. Review results above.' AS message;
