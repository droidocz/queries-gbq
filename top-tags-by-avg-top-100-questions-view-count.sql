-- Top tags by average questions view count for the top 100
-- Params:
-- - {{database}}: Database to be used on table creation

SELECT
  T.id,
  T.tag_name,
  T.count,
  T.excerpt_post_id,
  T.wiki_post_id,
  AVG(T.view_count) AS avg_view_count_top100
FROM (
  SELECT
    T.id,
    T.tag_name,
    T.count,
    T.excerpt_post_id,
    T.wiki_post_id,
    Q.view_count,
    RANK() OVER (PARTITION BY T.id ORDER BY Q.view_count DESC) AS `rank`
  FROM
    `{{database}}.tags` T
  INNER JOIN
    `{{database}}.posts_questions` Q
  ON
    (T.tag_name IN UNNEST(SPLIT(Q.tags, '|')))
  ORDER BY
    T.id ASC,
    Q.view_count DESC ) T
WHERE
  `rank` <= 100
GROUP BY T.id,
  T.tag_name,
  T.count,
  T.excerpt_post_id,
  T.wiki_post_id
HAVING COUNT(DISTINCT T.rank) >= 100
ORDER BY avg_view_count_top100 DESC
