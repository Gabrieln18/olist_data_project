WITH pedidos_por_vendedor AS (
    SELECT 
        oi.seller_id,
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    FROM delta_scan('../delta_lake/silver/order_items_silver') oi
    JOIN delta_scan('../delta_lake/silver/orders_full_data_silver') o 
        ON o.order_id = oi.order_id
),

reviews_por_vendedor AS (
    SELECT 
        oi.seller_id,
        r.review_score
    FROM delta_scan('../delta_lake/silver/order_items_silver') oi
    JOIN delta_scan('../delta_lake/silver/order_reviews') r 
        ON r.order_id = oi.order_id
),

agg_cancellation AS (
    SELECT 
        seller_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
        ROUND(SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS cancellation_rate
    FROM pedidos_por_vendedor
    GROUP BY seller_id
),

agg_reviews AS (
    SELECT 
        seller_id,
        COUNT(*) AS total_reviews,
        SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(SUM(CASE WHEN review_score IN (1, 2) THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS negative_review_rate
    FROM reviews_por_vendedor
    GROUP BY seller_id
),

agg_delivery_delay AS (
    SELECT 
        seller_id,
        ROUND(AVG(CAST(julian(order_delivered_customer_date) - julian(order_estimated_delivery_date) AS FLOAT)), 2) AS avg_delivery_delay
    FROM pedidos_por_vendedor
    WHERE order_delivered_customer_date IS NOT NULL
      AND order_estimated_delivery_date IS NOT NULL
    GROUP BY seller_id
),

agg_seller_lifetime AS (
    SELECT 
        seller_id,
        CAST(julian(MAX(order_purchase_timestamp)) - julian(MIN(order_purchase_timestamp)) AS INT) AS seller_lifetime_days
    FROM pedidos_por_vendedor
    GROUP BY seller_id
),

final_score AS (
    SELECT 
        c.seller_id,
        c.total_orders,
        c.canceled_orders,
        c.cancellation_rate,
        r.total_reviews,
        r.negative_reviews,
        r.negative_review_rate,
        d.avg_delivery_delay,
        l.seller_lifetime_days,

        -- Scoring por métrica
        CASE 
            WHEN c.cancellation_rate >= 0.2 THEN 3
            WHEN c.cancellation_rate >= 0.1 THEN 2
            ELSE 1
        END AS score_cancellation,

        CASE 
            WHEN r.negative_review_rate >= 0.3 THEN 3
            WHEN r.negative_review_rate >= 0.15 THEN 2
            ELSE 1
        END AS score_reviews,

        CASE 
            WHEN d.avg_delivery_delay >= 5 THEN 3
            WHEN d.avg_delivery_delay >= 2 THEN 2
            ELSE 1
        END AS score_delay,

        CASE 
            WHEN l.seller_lifetime_days < 30 THEN 1
            ELSE 0
        END AS score_inexperience,

        -- Soma dos scores
        (
            CASE 
                WHEN c.cancellation_rate >= 0.2 THEN 3
                WHEN c.cancellation_rate >= 0.1 THEN 2
                ELSE 1
            END
            +
            CASE 
                WHEN r.negative_review_rate >= 0.3 THEN 3
                WHEN r.negative_review_rate >= 0.15 THEN 2
                ELSE 1
            END
            +
            CASE 
                WHEN d.avg_delivery_delay >= 5 THEN 3
                WHEN d.avg_delivery_delay >= 2 THEN 2
                ELSE 1
            END
            +
            CASE 
                WHEN l.seller_lifetime_days < 30 THEN 1
                ELSE 0
            END
        ) AS total_risk_score,

        -- Classificação de risco
        CASE 
            WHEN (
                CASE 
                    WHEN c.cancellation_rate >= 0.2 THEN 3
                    WHEN c.cancellation_rate >= 0.1 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN r.negative_review_rate >= 0.3 THEN 3
                    WHEN r.negative_review_rate >= 0.15 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN d.avg_delivery_delay >= 5 THEN 3
                    WHEN d.avg_delivery_delay >= 2 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN l.seller_lifetime_days < 30 THEN 1
                    ELSE 0
                END
            ) >= 10 THEN 'HIGH'
            WHEN (
                CASE 
                    WHEN c.cancellation_rate >= 0.2 THEN 3
                    WHEN c.cancellation_rate >= 0.1 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN r.negative_review_rate >= 0.3 THEN 3
                    WHEN r.negative_review_rate >= 0.15 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN d.avg_delivery_delay >= 5 THEN 3
                    WHEN d.avg_delivery_delay >= 2 THEN 2
                    ELSE 1
                END
                +
                CASE 
                    WHEN l.seller_lifetime_days < 30 THEN 1
                    ELSE 0
                END
            ) >= 7 THEN 'MEDIAN'
            ELSE 'LOW'
        END AS risk_level

    FROM agg_cancellation c
    LEFT JOIN agg_reviews r ON c.seller_id = r.seller_id
    LEFT JOIN agg_delivery_delay d ON c.seller_id = d.seller_id
    LEFT JOIN agg_seller_lifetime l ON c.seller_id = l.seller_id
)

SELECT * 
FROM final_score
ORDER BY total_risk_score DESC, cancellation_rate DESC