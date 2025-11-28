WITH base AS (
  SELECT
    SAFE_CAST(museum_id AS INT64) AS museum_id,
    CASE
      WHEN LOWER(TRIM(day)) IN ('mon','monday') THEN 'Monday'
      WHEN LOWER(TRIM(day)) IN ('tue','tues','tuesday') THEN 'Tuesday'
      WHEN LOWER(TRIM(day)) IN ('wed','weds','wednesday') THEN 'Wednesday'
      WHEN LOWER(TRIM(day)) IN ('thu','thur','thurs','thursday') THEN 'Thursday'
      WHEN LOWER(TRIM(day)) IN ('fri','friday') THEN 'Friday'
      WHEN LOWER(TRIM(day)) IN ('sat','saturday') THEN 'Saturday'
      WHEN LOWER(TRIM(day)) IN ('sun','sunday') THEN 'Sunday'
      ELSE INITCAP(TRIM(CAST(day AS STRING)))
    END AS day,
    NULLIF(LOWER(TRIM(CAST(open  AS STRING))),  'closed') AS open_raw,
    NULLIF(LOWER(TRIM(CAST(close AS STRING))), 'closed') AS close_raw
  FROM `painting-475604.famous_painting.museum_hours`
),
norm AS (
  SELECT
    museum_id, day,
    REGEXP_REPLACE(REGEXP_REPLACE(open_raw,  r'[^0-9:apm]', ''),  r':?([ap]m)$', r'\1')  AS open_norm,
    REGEXP_REPLACE(REGEXP_REPLACE(close_raw, r'[^0-9:apm]', ''),  r':?([ap]m)$', r'\1')  AS close_norm
  FROM base
),
parsed AS (
  SELECT
    museum_id, day,
    SAFE.PARSE_TIME('%I:%M%p', open_norm)  AS open_time,
    SAFE.PARSE_TIME('%I:%M%p', close_norm) AS close_time
  FROM norm
),
dedup AS (
  SELECT
    museum_id, day,
    CASE WHEN open_time  IS NOT NULL THEN FORMAT_TIME('%H:%M', open_time)  END AS open,
    CASE WHEN close_time IS NOT NULL THEN FORMAT_TIME('%H:%M', close_time) END AS close,
    ROW_NUMBER() OVER (PARTITION BY museum_id, day ORDER BY museum_id) AS rn
  FROM parsed
)
