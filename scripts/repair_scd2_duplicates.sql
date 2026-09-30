-- One-off repair for snapshots.dim_user_snapshot (dbt SCD2) after two
-- `dbt snapshot` runs overlapped (2026-09-26 19:45:13 and 19:45:16) and gave
-- some users two current rows. dbt then updated every current row of such a
-- user separately on each later change, so the duplicates kept multiplying
-- in pairs. The dbt pool and max_active_runs=1 on DAG 04 now prevent overlap.
--
-- Keeps all history: exact duplicate rows (same dbt_scd_id) are reduced to
-- one, then any user still left with several current rows keeps only the
-- newest one current. Pause DAG 04 before running this.
--
--   set -a; source .env; set +a
--   docker compose exec -T warehouse-db psql -U "$WAREHOUSE_DB_USER" -d "$WAREHOUSE_DB_NAME" \
--       -v ON_ERROR_STOP=1 < scripts/repair_scd2_duplicates.sql

BEGIN;

SELECT count(*) AS users_with_multiple_current_rows_before
FROM (
    SELECT user_id_hash FROM snapshots.dim_user_snapshot
    WHERE dbt_valid_to IS NULL GROUP BY 1 HAVING count(*) > 1
) dup;

-- 1. Exact duplicates: dbt_scd_id = hash(user_id_hash, snapshot time), so
--    rows sharing it were written twice by the same snapshot. Keep one.
DELETE FROM snapshots.dim_user_snapshot a
USING snapshots.dim_user_snapshot b
WHERE a.dbt_scd_id = b.dbt_scd_id
  AND a.ctid > b.ctid;

-- 2. Rows from two different overlapping snapshots: close all but the newest.
WITH ranked AS (
    SELECT ctid,
           row_number() OVER (PARTITION BY user_id_hash ORDER BY dbt_valid_from DESC) AS rn,
           max(dbt_valid_from) OVER (PARTITION BY user_id_hash) AS newest_valid_from
    FROM snapshots.dim_user_snapshot
    WHERE dbt_valid_to IS NULL
)
UPDATE snapshots.dim_user_snapshot s
SET dbt_valid_to = r.newest_valid_from
FROM ranked r
WHERE s.ctid = r.ctid
  AND r.rn > 1;

SELECT count(*) AS users_with_multiple_current_rows_after
FROM (
    SELECT user_id_hash FROM snapshots.dim_user_snapshot
    WHERE dbt_valid_to IS NULL GROUP BY 1 HAVING count(*) > 1
) dup;

COMMIT;
