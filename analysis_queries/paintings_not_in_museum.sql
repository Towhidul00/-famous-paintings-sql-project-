SELECT
  w.work_id,
  w.name AS painting_name,
  a.full_name AS artist_name,
  w.style
FROM `painting-475604.famous_painting.work_cleaned` AS w
LEFT JOIN `painting-475604.famous_painting.artist_cleaned` AS a
  ON w.artist_id = a.artist_id
WHERE w.museum_id IS NULL;
