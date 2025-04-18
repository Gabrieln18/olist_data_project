SELECT 
    review_id,
    order_id,
    CAST(review_score AS INT) AS review_score,
    COALESCE(review_comment_title, 'no_title') AS review_comment_title,
    COALESCE(review_comment_message, 'no_message') AS review_comment_message,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
FROM delta_scan('../delta_lake/bronze/reviews_bronze');