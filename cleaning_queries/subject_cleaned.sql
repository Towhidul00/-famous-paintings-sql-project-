WITH base AS (
  SELECT
    SAFE_CAST(work_id AS INT64) AS work_id,
    NULLIF(TRIM(CAST(subject AS STRING)), '') AS subject_raw
  FROM `painting-475604.famous_painting.subject`
),
split AS (
  SELECT work_id, TRIM(part) AS subject_part
  FROM base,
  UNNEST(SPLIT(REGEXP_REPLACE(subject_raw, r'[;/|]', ','))) AS part
),
norm AS (
  SELECT work_id, LOWER(REGEXP_REPLACE(subject_part, r'\s+', ' ')) AS subject_lc
  FROM split
  WHERE subject_part IS NOT NULL
    AND LOWER(TRIM(subject_part)) NOT IN ('#value!','#value','na','n/a','null','-','--','unknown','untitled','')
),
mapped AS (
  SELECT
    work_id,
    CASE
      WHEN subject_lc = 'landscapes' THEN 'landscape'
      WHEN subject_lc = 'portraits'  THEN 'portrait'
      ELSE subject_lc
    END AS subject_lc
  FROM norm
),
valid AS (
  SELECT work_id, subject_lc
  FROM mapped
  WHERE work_id IS NOT NULL AND subject_lc IS NOT NULL AND subject_lc <> ''
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT work_id, INITCAP(subject_lc) AS subject,
           ROW_NUMBER() OVER (PARTITION BY work_id, subject_lc ORDER BY work_id) AS rn
    FROM valid
  )
  WHERE rn = 1
)
