BEGIN;

TRUNCATE TABLE mart.sales_items;

INSERT INTO mart.sales_items (
    order_item_id,
    order_id,
    user_id,
    email,
    full_name,
    order_status,
    is_delivered,
    order_date,
    order_created_at,
    product_id,
    product_title,
    category,
    subcategory,
    brand,
    color,
    sizes,
    material,
    quantity,
    unit_price,
    line_amount,
    city_id,
    city_name,
    country_iso,
    country_name,
    order_lat,
    order_lon,
    _refreshed_at
)
SELECT
    oi.id AS order_item_id,
    oi.order_id,
    o.user_id,
    u.email,
    u.full_name,
    o.status AS order_status,
    o.status = 'delivered' AS is_delivered,
    o.created_at::date AS order_date,
    o.created_at AS order_created_at,
    oi.product_id,
    p.title AS product_title,
    p.category,
    p.subcategory,
    p.brand,
    p.color,
    p.sizes,
    p.material,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_amount,
    o.city_id,
    c.name AS city_name,
    o.country_iso,
    co.name AS country_name,
    o.order_lat,
    o.order_lon,
    NOW() AS _refreshed_at
FROM raw.order_items oi
         JOIN raw.orders o
              ON o.id = oi.order_id
         JOIN raw.users u
              ON u.id = o.user_id
         JOIN raw.products p
              ON p.id = oi.product_id
         LEFT JOIN raw.cities c
                   ON c.id = o.city_id
         LEFT JOIN raw.countries co
                   ON co.iso_code = o.country_iso;

COMMIT;