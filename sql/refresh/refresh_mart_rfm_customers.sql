BEGIN;

TRUNCATE TABLE mart.rfm_customers;

WITH analysis_date AS (
    SELECT
        MAX(created_at)::date + 1 AS value
FROM raw.orders
WHERE status = 'delivered'
    ),

    rfm_base AS (
SELECT
    o.user_id,
    MAX(o.created_at) AS last_order_at,
    COUNT(DISTINCT o.id)::INTEGER AS frequency,
    SUM(o.total_amount)::NUMERIC(14, 2) AS monetary
FROM raw.orders o
WHERE o.status = 'delivered'
GROUP BY o.user_id
    ),

    rfm_metrics AS (
SELECT
    b.user_id,
    a.value AS analysis_date,
    b.last_order_at,
    (
    a.value - b.last_order_at::date
    )::INTEGER AS recency_days,
    b.frequency,
    b.monetary
FROM rfm_base b
    CROSS JOIN analysis_date a
    ),

    rfm_ranked AS (
SELECT
    m.*,

    PERCENT_RANK() OVER (
    ORDER BY m.recency_days ASC
    ) AS r_rank,

    PERCENT_RANK() OVER (
    ORDER BY m.frequency ASC
    ) AS f_rank,

    PERCENT_RANK() OVER (
    ORDER BY m.monetary ASC
    ) AS m_rank

FROM rfm_metrics m
    ),

    rfm_scored AS (
SELECT
    r.*,

    CASE
    WHEN r.r_rank <= 0.20 THEN 5
    WHEN r.r_rank <= 0.40 THEN 4
    WHEN r.r_rank <= 0.60 THEN 3
    WHEN r.r_rank <= 0.80 THEN 2
    ELSE 1
    END::SMALLINT AS r_score,

    CASE
    WHEN r.f_rank >= 0.80 THEN 5
    WHEN r.f_rank >= 0.60 THEN 4
    WHEN r.f_rank >= 0.40 THEN 3
    WHEN r.f_rank >= 0.20 THEN 2
    ELSE 1
    END::SMALLINT AS f_score,

    CASE
    WHEN r.m_rank >= 0.80 THEN 5
    WHEN r.m_rank >= 0.60 THEN 4
    WHEN r.m_rank >= 0.40 THEN 3
    WHEN r.m_rank >= 0.20 THEN 2
    ELSE 1
    END::SMALLINT AS m_score

FROM rfm_ranked r
    ),

    rfm_segmented AS (
SELECT
    s.*,

    CONCAT(
    s.r_score,
    s.f_score,
    s.m_score
    ) AS rfm_code,

    CASE
    WHEN s.r_score >= 4
    AND s.f_score >= 4
    AND s.m_score >= 4
    THEN 'Champions'

    WHEN s.r_score >= 3
    AND s.f_score >= 4
    THEN 'Loyal Customers'

    WHEN s.r_score = 5
    AND s.f_score = 1
    THEN 'New Customers'

    WHEN s.r_score >= 4
    AND s.f_score BETWEEN 2 AND 3
    THEN 'Potential Loyalists'

    WHEN s.r_score <= 2
    AND s.f_score >= 3
    THEN 'At Risk'

    WHEN s.r_score = 1
    AND s.f_score <= 2
    THEN 'Lost Customers'

    WHEN s.r_score >= 3
    AND s.m_score >= 4
    AND s.f_score <= 2
    THEN 'Big Spenders'

    ELSE 'Regular Customers'
    END AS segment

FROM rfm_scored s
    )

INSERT INTO mart.rfm_customers (
    user_id,
    email,
    full_name,
    analysis_date,
    last_order_at,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_code,
    segment,
    _refreshed_at
)
SELECT
    r.user_id,
    u.email,
    u.full_name,
    r.analysis_date,
    r.last_order_at,
    r.recency_days,
    r.frequency,
    r.monetary,
    r.r_score,
    r.f_score,
    r.m_score,
    r.rfm_code,
    r.segment,
    NOW()
FROM rfm_segmented r
         JOIN raw.users u
              ON u.id = r.user_id;

COMMIT;