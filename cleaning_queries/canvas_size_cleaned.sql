WITH base AS (
  SELECT
    SAFE_CAST(size_id AS INT64) AS size_id,
    SAFE_CAST(height  AS INT64) AS height,
    SAFE_CAST(width   AS INT64) AS width,
    CASE
      WHEN label IS NULL THEN NULL
      ELSE LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(label AS STRING)), r'\s+', ' '), r'[”″]', '"'))
    END AS label
  FROM `painting-475604.famous_painting.canvas_size`
),
parsed AS (
  SELECT
    *,
    SAFE_CAST(REGEXP_EXTRACT(label, r'[x×]\s*(\d+(?:\.\d+)?)\s*"') AS FLOAT64) AS h_in,
    SAFE_CAST(REGEXP_EXTRACT(label, r'(\d+(?:\.\d+)?)\s*"\s*[x×]') AS FLOAT64) AS w_in
  FROM base
),
filled AS (
  SELECT
    size_id,
    COALESCE(height, CAST(ROUND(h_in) AS INT64)) AS height,
    COALESCE(width , CAST(ROUND(w_in) AS INT64)) AS width,
    label
  FROM parsed
),
valid AS (
  SELECT * FROM filled
  WHERE size_id IS NOT NULL
    AND (height IS NULL OR height > 0)
    AND (width  IS NULL OR width  > 0)
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY size_id
        ORDER BY (height IS NOT NULL AND width IS NOT NULL) DESC, size_id
      ) AS rn
    FROM valid
  )
  WHERE rn = 1
)
