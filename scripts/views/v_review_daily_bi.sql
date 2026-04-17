CREATE OR REPLACE VIEW `amazon-books-de-pipeline.amazon_serve_gold.v_review_daily_bi` AS
SELECT
  review_date,
  COUNT(*) AS review_count,
  COUNT(DISTINCT title_hash) AS reviewed_book_count,
  ROUND(AVG(review_score), 2) AS avg_review_score,
  ROUND(AVG(price), 2) AS avg_price,
  COUNTIF(price IS NOT NULL) AS priced_review_count
FROM `amazon-books-de-pipeline.amazon_serve_gold.books_rating`
WHERE review_date IS NOT NULL
GROUP BY review_date;