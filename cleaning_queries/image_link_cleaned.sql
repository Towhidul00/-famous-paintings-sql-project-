WITH base AS (
  SELECT
    SAFE_CAST(work_id AS INT64) AS work_id,
    NULLIF(TRIM(CAST(url AS STRING)), '')                 AS url_raw,
    NULLIF(TRIM(CAST(thumbnail_small_url AS STRING)), '') AS small_raw,
    NULLIF(TRIM(CAST(thumbnail_large_url AS STRING)), '') AS large_raw
  FROM `painting-475604.famous_painting.image_link`
),
proto_norm AS (
  SELECT
    work_id,
    REGEXP_REPLACE(url_raw,   r'^(?i)//', 'https://') AS url1,
    REGEXP_REPLACE(small_raw, r'^(?i)//', 'https://') AS small1,
    REGEXP_REPLACE(large_raw, r'^(?i)//', 'https://') AS large1
  FROM base
),
https_pref AS (
  SELECT
    work_id,
    IF(REGEXP_CONTAINS(url1,   r'^(?i)http://'), REGEXP_REPLACE(url1,   r'^(?i)http://', 'https://'), url1)   AS url,
    IF(REGEXP_CONTAINS(small1, r'^(?i)http://'), REGEXP_REPLACE(small1, r'^(?i)http://', 'https://'), small1) AS thumbnail_small_url,
    IF(REGEXP_CONTAINS(large1, r'^(?i)http://'), REGEXP_REPLACE(large1, r'^(?i)http://', 'https://'), large1) AS thumbnail_large_url
  FROM proto_norm
),
valid AS (
  SELECT *
  FROM https_pref
  WHERE work_id IS NOT NULL AND work_id > 0
    AND (url IS NOT NULL OR thumbnail_small_url IS NOT NULL OR thumbnail_large_url IS NOT NULL)
    AND (url IS NULL OR REGEXP_CONTAINS(url, r'^(?i)https?://'))
    AND (thumbnail_small_url IS NULL OR REGEXP_CONTAINS(thumbnail_small_url, r'^(?i)https?://'))
    AND (thumbnail_large_url IS NULL OR REGEXP_CONTAINS(thumbnail_large_url, r'^(?i)https?://'))
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY work_id
        ORDER BY (thumbnail_large_url IS NOT NULL) DESC, (thumbnail_small_url IS NOT NULL) DESC, (url IS NOT NULL) DESC
      ) AS rn
    FROM valid
  )
  WHERE rn = 1
)
