-- Get Top 100 questions and their data for each of the specified tags
-- Params:
-- - {{tags}}: Tags to filter (e.g.: ['android-layout', 'android-activity', 'android-intent', 'android-edittext', 'android-fragments', 'android-recyclerview', 'listview', 'android-actionbar', 'google-maps', 'android-asynctask'])
-- - {{database}}: Database to be used on table creation (e.g.: android_by_tags)
CREATE OR REPLACE TABLE
  `{{database}}.posts_questions` AS
WITH
  tags_to_use AS (
  SELECT
    *
  FROM
    UNNEST({{tags}}) AS tag -- define what tags will be used
  WITH
  OFFSET
    AS
  OFFSET
  ORDER BY
  OFFSET
    ),
  android_how_to_questions AS (
  SELECT
    PQ.*
  FROM
    `bigquery-public-data.stackoverflow.posts_questions` PQ
  WHERE
    'android' IN UNNEST(SPLIT(PQ.tags, "|")) -- only with actual tag android
    AND (PQ.title LIKE '%how%'
      OR PQ.body LIKE '%how%') -- it has "how" in the title or body
    AND (PQ.body NOT LIKE '%fail%'
      AND PQ.body NOT LIKE '%problem%'
      AND PQ.body NOT LIKE '%error%'
      AND PQ.body NOT LIKE '%wrong%'
      AND PQ.body NOT LIKE '%fix%'
      AND PQ.body NOT LIKE '%bug%'
      AND PQ.body NOT LIKE '%issue%'
      AND PQ.body NOT LIKE '%solve%'
      AND PQ.body NOT LIKE '%trouble%')-- AND it doesn't have any of the debug-corrective words in the body (“fail”, “problem”, “error”, “wrong”, “fix”, “bug”, “issue”, “solve”, “trouble”)
    -- AND it doesn't have "error" in any of the code snippets (also tracked by the above)
  ORDER BY
    PQ.view_count DESC)
SELECT
  *
FROM (
  SELECT
    T.id AS tag_id,
    TTU.
  OFFSET
    AS tag_offset,
    T.tag_name,
    T.wiki_post_id AS tag_wiki_post_id,
    RANK() OVER (PARTITION BY T.id ORDER BY Q.view_count DESC) AS `question_view_count_rank`,
    COUNT(Q.id) OVER (PARTITION BY T.id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `total_valid_questions`,
    Q.*
  FROM
    `bigquery-public-data.stackoverflow.tags` T
  INNER JOIN
    tags_to_use TTU
  ON
    (TTU.tag = T.tag_name)
  INNER JOIN
    android_how_to_questions Q
  ON
    (T.tag_name IN UNNEST(SPLIT(Q.tags, '|')))
  ORDER BY
    TTU.
  OFFSET
    ASC,
    Q.view_count DESC ) T
WHERE
  `question_view_count_rank` <= 100
  AND `total_valid_questions` >= 100;


-- [Create Posts Answers Table]
CREATE OR REPLACE TABLE
  `{{database}}.posts_answers` AS
SELECT
  PA.*
FROM
  `bigquery-public-data.stackoverflow.posts_answers` PA
WHERE
  PA.parent_id IN (
  SELECT
    id
  FROM
    `{{database}}.posts_questions`);

-- [Create Questions Table]
CREATE OR REPLACE TABLE
  `{{database}}.comments` AS
SELECT
  C.*
FROM
  `bigquery-public-data.stackoverflow.comments` C
WHERE
  C.post_id IN (
  SELECT
    id
  FROM
    `{{database}}.posts_questions`
  UNION ALL
  SELECT
    id
  FROM
    `{{database}}.posts_answers`);

-- [Create Users Table]
CREATE OR REPLACE TABLE
  `{{database}}.users` AS
SELECT
  U.*
FROM
  `bigquery-public-data.stackoverflow.users` U
WHERE
  U.id IN (
  SELECT
    owner_user_id
  FROM
    `{{database}}.posts_questions`
  UNION ALL
  SELECT
    owner_user_id
  FROM
    `{{database}}.posts_answers`
  UNION ALL
  SELECT
    user_id
  FROM
    `{{database}}.comments`);

-- [Create Tags Table]
CREATE OR REPLACE TABLE `{{database}}.tags` AS
SELECT
  T.*
FROM
  `bigquery-public-data.stackoverflow.tags` T
WHERE
  T.tag_name IN (
  WITH
    sequences AS (
    SELECT
      SPLIT(tags, '|') AS tags
    FROM
      `{{database}}.posts_questions`)
  SELECT
    DISTINCT flattened_tags
  FROM
    sequences
  CROSS JOIN
    UNNEST(sequences.tags) AS flattened_tags)
