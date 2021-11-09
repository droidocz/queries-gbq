  -- Params:
  -- - {{database}}: Database to be used on table creation

-- [Create Posts Questions Table] (Main query)
CREATE OR REPLACE TABLE
  `{{database}}.posts_questions` AS
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
  PQ.view_count DESC;

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
