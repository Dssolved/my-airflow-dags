DROP TABLE IF EXISTS mart.warehouse_candidates;

CREATE TABLE mart.warehouse_candidates AS
WITH delivered_orders AS (
    SELECT
        o.id          AS order_id,
        o.user_id,
        o.total_amount,
        o.city_id,
        o.country_iso,
        o.order_lat,
        o.order_lon,
        oi.quantity
    FROM raw.orders o
             JOIN raw.order_items oi ON oi.order_id = o.id
    WHERE o.status = 'delivered'
      AND o.order_lat IS NOT NULL
      AND o.order_lon IS NOT NULL
),

     city_stats AS (
         SELECT
             o.city_id,
             c.name                          AS city_name,
             c.country_iso,
             co.name                         AS country_name,
             c.lat                           AS city_lat,
             c.lon                           AS city_lon,
             COUNT(DISTINCT o.order_id)      AS delivered_orders,
             SUM(o.total_amount)             AS revenue,
             COUNT(DISTINCT o.user_id)       AS customers,
             SUM(o.quantity)                 AS units_sold,
             SUM(o.total_amount) /
             NULLIF(COUNT(DISTINCT o.order_id), 0) AS avg_order_value
         FROM delivered_orders o
                  JOIN raw.cities c   ON c.id = o.city_id
                  JOIN raw.countries co ON co.iso_code = o.country_iso
         GROUP BY o.city_id, c.name, c.country_iso, co.name, c.lat, c.lon
     ),

     totals AS (
         SELECT
             SUM(delivered_orders) AS total_orders,
             SUM(revenue)          AS total_revenue
         FROM city_stats
     ),

     distances AS (
         SELECT
             cs.city_id,
             AVG(
                     6371 * 2 * ASIN(
                             SQRT(
                                     POWER(SIN(RADIANS((o.order_lat - cs.city_lat) / 2)), 2)
                                         + COS(RADIANS(cs.city_lat))
                                         * COS(RADIANS(o.order_lat))
                                         * POWER(SIN(RADIANS((o.order_lon - cs.city_lon) / 2)), 2)
                             )
                                )
             ) AS avg_distance_km
         FROM city_stats cs
                  CROSS JOIN delivered_orders o
         GROUP BY cs.city_id
     ),

     combined AS (
         SELECT
             cs.country_iso,
             cs.country_name,
             cs.city_id,
             cs.city_name,
             cs.delivered_orders,
             cs.revenue,
             cs.customers,
             cs.units_sold,
             ROUND(cs.avg_order_value::NUMERIC, 2)           AS avg_order_value,
             ROUND(cs.delivered_orders::NUMERIC /
            t.total_orders * 100, 2)                    AS order_share,
             ROUND(cs.revenue / t.total_revenue * 100, 2)   AS revenue_share,
             ROUND(d.avg_distance_km::NUMERIC, 2)            AS avg_distance_km
         FROM city_stats cs
                  JOIN distances d ON d.city_id = cs.city_id
                  CROSS JOIN totals t
     ),

     normalized AS (
         SELECT
             *,
             (delivered_orders - MIN(delivered_orders) OVER ()) /
             NULLIF(MAX(delivered_orders) OVER () -
                                          MIN(delivered_orders) OVER (), 0) AS norm_orders,
             (revenue - MIN(revenue) OVER ()) /
             NULLIF(MAX(revenue) OVER () -
                                 MIN(revenue) OVER (), 0)          AS norm_revenue,
             (customers - MIN(customers) OVER ()) /
             NULLIF(MAX(customers) OVER () -
                                   MIN(customers) OVER (), 0)        AS norm_customers,
             1 - (avg_distance_km - MIN(avg_distance_km) OVER ()) /
                 NULLIF(MAX(avg_distance_km) OVER () -
                                             MIN(avg_distance_km) OVER (), 0)  AS norm_distance
         FROM combined
     )

SELECT
    country_iso,
    country_name,
    city_id,
    city_name,
    delivered_orders,
    ROUND(revenue::NUMERIC, 2)          AS revenue,
    customers,
    units_sold,
    avg_order_value,
    order_share,
    revenue_share,
    avg_distance_km,
    ROUND((
              0.55 * norm_orders +
              0.30 * norm_revenue +
              0.15 * norm_customers
              )::NUMERIC, 4)                      AS demand_score,
    ROUND(norm_distance::NUMERIC, 4)    AS distance_score,
    ROUND((
              0.45 * norm_orders +
              0.30 * norm_revenue +
              0.15 * norm_customers +
              0.10 * norm_distance
              )::NUMERIC, 4)                      AS final_score,
    RANK() OVER (ORDER BY (
        0.45 * norm_orders +
        0.30 * norm_revenue +
        0.15 * norm_customers +
        0.10 * norm_distance
    ) DESC)                             AS candidate_rank
FROM normalized
ORDER BY candidate_rank;