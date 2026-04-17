CREATE OR REPLACE VIEW `amazon-books-de-pipeline.amazon_serve_gold.v_book_performance_bi` AS
WITH rating_agg AS (
  SELECT
    title_hash,
    COUNT(*) AS review_count,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    MIN(review_score) AS min_review_score,
    MAX(review_score) AS max_review_score,
    MIN(review_date) AS first_review_date,
    MAX(review_date) AS last_review_date,
    ROUND(AVG(price), 2) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
  FROM `amazon-books-de-pipeline.amazon_serve_gold.books_rating`
  GROUP BY title_hash
)
SELECT
  bd.title_hash,
  bd.title,
  REPLACE(REPLACE(REPLACE(bd.authors, "[", ""), "]", ""), "'", "") AS authors,
  bd.publisher,
  bd.published_date,
  REPLACE(REPLACE(bd.categories, "['", ""), "']", "") AS categories,
  bd.ratings_count,
  bd.completeness_score,
  COALESCE(ra.review_count, 0) AS review_count,
  ra.avg_review_score,
  ra.min_review_score,
  ra.max_review_score,
  ra.first_review_date,
  ra.last_review_date,
  ra.avg_price,
  ra.min_price,
  ra.max_price,
  CASE
    WHEN COALESCE(ra.review_count, 0) = 0 THEN 'No Reviews'
    WHEN ra.avg_review_score >= 4.5 THEN 'Excellent'
    WHEN ra.avg_review_score >= 4.0 THEN 'Good'
    WHEN ra.avg_review_score >= 3.0 THEN 'Average'
    ELSE 'Low'
  END AS rating_band
FROM `amazon-books-de-pipeline.amazon_serve_gold.books_data` bd
LEFT JOIN rating_agg ra
  ON bd.title_hash = ra.title_hash;