WITH base AS (
  SELECT
    SAFE_CAST(work_id AS INT64)                       AS work_id,
    CASE
      WHEN LOWER(TRIM(CAST(size_id AS STRING))) IN ('#value!','#value','na','n/a','null','-','--','') THEN NULL
      ELSE REGEXP_EXTRACT(TRIM(CAST(size_id AS STRING)), r'\d+')
    END AS size_id,
    SAFE_CAST(sale_price AS INT64)    AS sale_raw,
    SAFE_CAST(regular_price AS INT64) AS regular_raw
  FROM `painting-475604.famous_painting.product_size`
),
filled AS (
  SELECT
    work_id,
    size_id,
    COALESCE(sale_raw,    regular_raw) AS sale_price,
    COALESCE(regular_raw, sale_raw)    AS regular_price
  FROM base
),
fixed AS (
  SELECT
    work_id, size_id,
    CASE WHEN sale_price IS NOT NULL AND regular_price IS NOT NULL AND sale_price > regular_price
      THEN regular_price ELSE sale_price END AS sale_price,
    CASE WHEN sale_price IS NOT NULL AND regular_price IS NOT NULL AND sale_price > regular_price
      THEN sale_price ELSE regular_price END AS regular_price
  FROM filled
),
valid AS (
  SELECT *
  FROM fixed
  WHERE work_id IS NOT NULL AND size_id IS NOT NULL
    AND (sale_price IS NULL OR sale_price > 0)
    AND (regular_price IS NULL OR regular_price > 0)
    AND (sale_price IS NOT NULL OR regular_price IS NOT NULL)
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY work_id, size_id
        ORDER BY (sale_price IS NOT NULL) DESC, (regular_price IS NOT NULL) DESC, regular_price DESC
      ) AS rn
    FROM valid
  )
  WHERE rn = 1
)
