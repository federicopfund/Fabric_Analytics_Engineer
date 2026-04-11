CREATE OR REPLACE VIEW ecommerce.avg_discount AS
SELECT
    AVG(promotion.discount_percentage) AS avg_discount
FROM
    ecommerce.order_promotions op
INNER JOIN
    ecommerce.promotions promotion ON op.promotion_id = promotion.promotion_id;
