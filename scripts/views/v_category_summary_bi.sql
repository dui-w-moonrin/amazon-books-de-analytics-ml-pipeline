CREATE OR REPLACE VIEW `amazon-books-de-pipeline.amazon_serve_gold.v_category_summary_bi` AS
WITH book_base AS (
  SELECT
    bd.title_hash,
    REPLACE(REPLACE(bd.categories, "['", ""), "']", "") AS categories,
    bd.title,
    br.review_score
  FROM `amazon-books-de-pipeline.amazon_serve_gold.books_data` bd
  LEFT JOIN `amazon-books-de-pipeline.amazon_serve_gold.books_rating` br
    ON bd.title_hash = br.title_hash
)
SELECT
  categories,
  COUNT(DISTINCT title_hash) AS book_count,
  COUNT(review_score) AS review_count,
  ROUND(AVG(review_score), 2) AS avg_review_score
FROM book_base
GROUP BY categories;