CREATE OR REPLACE VIEW ecommerce.high_discount_orders AS
SELECT
    o.order_id,
    o.order_date,
    op.promotion_id,
    promotion.discount_percentage
FROM
    ecommerce.orders o
INNER JOIN
    ecommerce.order_promotions op ON o.order_id = op.order_id
INNER JOIN
    ecommerce.promotions promotion ON op.promotion_id = promotion.promotion_id
WHERE
    promotion.discount_percentage > 20;
