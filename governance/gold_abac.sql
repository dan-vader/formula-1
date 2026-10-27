USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:gold_schema);

ALTER MATERIALIZED VIEW fact_transaction_lap ALTER COLUMN team_sk SET TAGS ('f1_access_key' = 'team');
ALTER MATERIALIZED VIEW fact_transaction_lap ALTER COLUMN tyre_age_laps SET TAGS ('f1_sensitivity' = 'confidential');
ALTER MATERIALIZED VIEW fact_transaction_lap ALTER COLUMN stint_number SET TAGS ('f1_sensitivity' = 'confidential');

CREATE OR REPLACE FUNCTION team_rows(p_team_sk BIGINT)
RETURNS BOOLEAN
RETURN
  NOT EXISTS (
    SELECT 1 FROM dim_team d
    WHERE is_account_group_member(concat('f1_team_', d.constructor_id))
  )
  OR EXISTS (
    SELECT 1 FROM dim_team d
    WHERE d.team_sk = p_team_sk
      AND is_account_group_member(concat('f1_team_', d.constructor_id))
  );

CREATE OR REPLACE FUNCTION mask_strategy(val VARIANT)
RETURNS VARIANT
DETERMINISTIC
RETURN NULL::VARIANT;

CREATE OR REPLACE POLICY team_rows
ON SCHEMA IDENTIFIER(:gold_schema)
COMMENT 'Team engineers see only rows of their own team'
ROW FILTER team_rows
TO `account users`
EXCEPT `f1_analysts`, `f1_pipeline_runners`
FOR TABLES
MATCH COLUMNS has_tag_value('f1_access_key', 'team') AS team
USING COLUMNS (team);

CREATE OR REPLACE POLICY mask_strategy
ON SCHEMA IDENTIFIER(:gold_schema)
COMMENT 'Mask confidential columns for everyone outside analysts and team engineers'
COLUMN MASK mask_strategy
TO `account users`
EXCEPT `f1_analysts`, `f1_pipeline_runners`, `f1_team_ferrari`
FOR TABLES
MATCH COLUMNS has_tag_value('f1_sensitivity', 'confidential') AS col
ON COLUMN col;