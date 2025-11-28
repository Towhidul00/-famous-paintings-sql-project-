WITH base AS (
  SELECT
    SAFE_CAST(museum_id AS INT64) AS museum_id,
    REGEXP_REPLACE(TRIM(CAST(name    AS STRING)), r'\s+', ' ') AS name_raw,
    REGEXP_REPLACE(TRIM(CAST(address AS STRING)), r'\s+', ' ') AS address_raw,
    REGEXP_REPLACE(TRIM(CAST(city    AS STRING)), r'\s+', ' ') AS city_raw,
    REGEXP_REPLACE(TRIM(CAST(state   AS STRING)), r'\s+', ' ') AS state_raw,
    REGEXP_REPLACE(TRIM(CAST(postal  AS STRING)), r'\s+', ' ') AS postal_raw,
    REGEXP_REPLACE(TRIM(CAST(country AS STRING)), r'\s+', ' ') AS country_raw,
    NULLIF(TRIM(CAST(phone AS STRING)), '') AS phone_raw,
    NULLIF(TRIM(CAST(url   AS STRING)), '') AS url_raw
  FROM `painting-475604.famous_painting.museum`
),
std_text AS (
  SELECT
    museum_id,
    INITCAP(REGEXP_REPLACE(name_raw,    r'[,\.;:\- ]+$', '')) AS name,
    INITCAP(REGEXP_REPLACE(address_raw, r'[,\.;:\- ]+$', '')) AS address,
    CASE WHEN REGEXP_CONTAINS(city_raw, r'^[0-9\-\s]+$') THEN NULL
         ELSE INITCAP(REGEXP_REPLACE(city_raw, r'[,\.;:\- ]+$', '')) END AS city,
    CASE
      WHEN REGEXP_CONTAINS(state_raw, r'^[0-9]+$') THEN NULL
      WHEN REGEXP_CONTAINS(state_raw, r'^[A-Z0-9]{3,8}$') THEN NULL
      WHEN REGEXP_CONTAINS(state_raw, r'^[A-Za-z]{1,3}$') THEN UPPER(state_raw)
      ELSE INITCAP(REGEXP_REPLACE(state_raw, r'[,\.;:\- ]+$', ''))
    END AS state,
    REGEXP_REPLACE(UPPER(postal_raw), r'[\s-]', '') AS postal,
    CASE
      WHEN LOWER(TRIM(country_raw)) IN ('united kingdom','great britain','england') THEN 'UK'
      WHEN LOWER(TRIM(country_raw)) IN ('united states','united states of america','usa') THEN 'USA'
      ELSE INITCAP(REGEXP_REPLACE(country_raw, r'[,\.;:\- ]+$', ''))
    END AS country,
    phone_raw, url_raw
  FROM base
),
contact_norm AS (
  SELECT
    museum_id, name, address, city, state, postal, country,
    REGEXP_REPLACE(phone_raw, r'[^0-9+]', '') AS phone,
    CASE
      WHEN url_raw IS NULL THEN NULL
      WHEN REGEXP_CONTAINS(url_raw, r'^(?i)https?://') THEN url_raw
      WHEN REGEXP_CONTAINS(url_raw, r'^(?i)//') THEN CONCAT('https:', url_raw)
      ELSE CONCAT('https://', url_raw)
    END AS url
  FROM std_text
),
valid AS (
  SELECT *
  FROM contact_norm
  WHERE museum_id IS NOT NULL AND museum_id > 0
    AND name IS NOT NULL AND country IS NOT NULL
    AND (city  IS NULL OR NOT REGEXP_CONTAINS(city,  r'^[0-9\-\s]+$'))
    AND (state IS NULL OR NOT REGEXP_CONTAINS(state, r'^[0-9A-Z]{3,8}$'))
    AND (url IS NULL OR REGEXP_CONTAINS(url, r'^(?i)https?://'))
),
dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY museum_id
        ORDER BY (url IS NOT NULL) DESC, (phone IS NOT NULL) DESC, name
      ) AS rn
    FROM valid
  )
  WHERE rn = 1
)
