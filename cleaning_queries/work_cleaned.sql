WITH base AS (
  SELECT
    SAFE_CAST(work_id AS INT64)   AS work_id,
    INITCAP(NULLIF(REGEXP_REPLACE(TRIM(CAST(name AS STRING)), r'\s+', ' '), '')) AS name,
    SAFE_CAST(artist_id AS INT64) AS artist_id,
    CASE
      WHEN LOWER(TRIM(CAST(style AS STRING))) IN ('#value!','#value','na','n/a','null','-','--','') THEN NULL
      ELSE LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(style AS STRING)), r'\s+', ' '), r'[,\.;:\- ]+$', ''))
    END AS style,
    SAFE_CAST(museum_id AS INT64) AS museum_id
  FROM `painting-475604.famous_painting.work`
),
style_norm AS (
  SELECT
    work_id, name, artist_id,
    CASE
      WHEN style IN ('post impressionism','post-impressionism','post impressionist') THEN 'post-impressionism'
      WHEN style IN ('impressionist','impressionists') THEN 'impressionism'
      WHEN style IN ('baroque period') THEN 'baroque'
      ELSE style
    END AS style,
    museum_id
  FROM base
),
valid AS (
  SELECT *
  FROM style_norm
  WHERE work_id IS NOT NULL AND work_id > 0
    AND artist_id IS NOT NULL AND artist_id > 0
    AND (museum_id IS NULL OR museum_id > 0)
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY work_id
        ORDER BY (name IS NOT NULL) DESC, (style IS NOT NULL) DESC, (museum_id IS NOT NULL) DESC, work_id
      ) AS rn
    FROM valid
  )
  WHERE rn = 1
)
