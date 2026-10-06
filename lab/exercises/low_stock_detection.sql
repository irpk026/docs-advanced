-- Exercise 1 — LOW_STOCK detection
--
-- A second detector that fires on low ABSOLUTE stock, independent of how fast
-- the product is selling. Writes to the same output topic as the velocity
-- detector, so the existing consumer and agent pick it up with no changes.
--
-- Run this in the Flink SQL workspace ALONGSIDE velocity_anomaly_detection.sql.
-- Both are long-running INSERT jobs writing to fashion.velocity.anomalies.
--
-- Column list and types must match the velocity detector exactly — the output
-- topic's schema has "additionalProperties": false and a fixed required list.

INSERT INTO `fashion.velocity.anomalies` (
    `key`,
    `alertId`,
    `anomalyType`,
    `severity`,
    `timestamp`,
    `storeId`,
    `productId`,
    `sku`,
    `size`,
    `color`,
    `category`,
    `brand`,
    `currentVelocity`,
    `baselineVelocity`,
    `velocityRatio`,
    `currentStock`,
    `hoursToStockout`,
    `estimatedValue`,
    `recommendation`
)
SELECT
    CAST(sku AS BYTES) AS key,
    CONCAT('LOWSTOCK-', CAST(UNIX_TIMESTAMP() AS STRING), '-', sku) as alertId,

    -- 'LOW_STOCK' is already in the schema's anomalyType enum alongside
    -- VELOCITY_SPIKE and SEASONAL_ANOMALY, so no schema change is needed.
    'LOW_STOCK' as anomalyType,

    -- Severity comes from how little is left, not from sales speed.
    CASE
        WHEN quantityAfter <= 5  THEN 'CRITICAL'
        WHEN quantityAfter <= 15 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,

    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS `timestamp`,
    storeId,
    productId,
    sku,
    size,
    color,
    category,
    brand,

    -- This alert is not velocity-driven, so report the demo baseline for both
    -- and a ratio of 1.0 — the agent reads anomalyType to tell them apart.
    CAST(2.0 AS DOUBLE) as currentVelocity,
    CAST(2.0 AS DOUBLE) as baselineVelocity,
    CAST(1.0 AS DOUBLE) as velocityRatio,

    quantityAfter as currentStock,
    CAST(quantityAfter / 2 AS INT) as hoursToStockout,   -- at the 2.0/h baseline
    CAST(quantityAfter * unitPrice AS DOUBLE) as estimatedValue,
    'REORDER_RECOMMENDED' as recommendation
FROM `fashion.inventory.events`
WHERE quantityAfter <= 25        -- the low-stock threshold
    AND quantityAfter > 0        -- already-zero stock is a different problem
    AND eventType IN ('SALE', 'ADJUSTMENT');
