CREATE VIEW v_race_classification_current
COMMENT 'Latest classification version per session and driver; points_change is summed on the fact across versions'
AS SELECT
  session_sk,
  driver_sk,
  classification_version,
  session_date_sk,
  classification_date_sk,
  finish_status_sk,
  team_sk,
  grid_position,
  classified_position,
  points,
  overtakes_made
FROM fact_transaction_race_classification
QUALIFY row_number() OVER (PARTITION BY session_sk, driver_sk ORDER BY classification_version DESC) = 1;