WITH base AS (
  SELECT
    SAFE_CAST(artist_id AS INT64) AS artist_id,
    INITCAP(NULLIF(TRIM(full_name), ''))    AS full_name,
    INITCAP(NULLIF(TRIM(first_name), ''))   AS first_name,
    INITCAP(NULLIF(TRIM(middle_names), '')) AS middle_names,
    INITCAP(NULLIF(TRIM(last_name), ''))    AS last_name,
    LOWER(NULLIF(TRIM(nationality), ''))    AS nationality,
    LOWER(NULLIF(TRIM(style), ''))          AS style,
    SAFE_CAST(birth AS INT64)               AS birth,
    SAFE_CAST(death AS INT64)               AS death
  FROM `painting-475604.famous_painting.artist`
),
names_filled AS (
  SELECT
    artist_id,
    COALESCE(
      full_name,
      ARRAY_TO_STRING(
        ARRAY(SELECT p FROM UNNEST([first_name, middle_names, last_name]) p WHERE p IS NOT NULL AND p <> ''), ' '
      )
    ) AS full_name,
    first_name, middle_names, last_name, nationality, style, birth, death
  FROM base
),
valid AS (
  SELECT *
  FROM names_filled
  WHERE artist_id IS NOT NULL AND artist_id > 0
    AND full_name IS NOT NULL
    AND birth IS NOT NULL AND birth BETWEEN 1000 AND 2025
    AND (death IS NULL OR death BETWEEN birth AND 2025)
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id) AS rn
    FROM valid
  )
  WHERE rn = 1
)
