BEGIN;

TRUNCATE TABLE mart.orders;

INSERT INTO mart.orders (
    order_id,
    user_id,
    email,
    full_name,
    status,
    is_delivered,
    total_amount,
    order_date,
    created_at,
    city_id,
    city_name,
    country_iso,
    country_name,
    order_lat,
    order_lon,
    _refreshed_at
)
SELECT
    o.id,
    o.user_id,
    u.email,
    u.full_name,
    o.status,
    o.status = 'delivered',
    o.total_amount,
    o.created_at::date,
    o.created_at,
    o.city_id,
    c.name,
    o.country_iso,
    co.name,
    o.order_lat,
    o.order_lon,
    NOW()
FROM raw.orders o
         JOIN raw.users u
              ON u.id = o.user_id
         LEFT JOIN raw.cities c
                   ON c.id = o.city_id
         LEFT JOIN raw.countries co
                   ON co.iso_code = o.country_iso;

COMMIT;