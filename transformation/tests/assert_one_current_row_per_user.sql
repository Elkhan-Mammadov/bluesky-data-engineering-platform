-- Singular dbt test (CLAUDE.md 8.8: "Exactly one current row per user in
-- SCD2 -> DAG fails"). A dbt test fails if this query returns any rows,
-- so we select the users that (wrongly) have more than one current row.
select
    user_id_hash,
    count(*) as current_row_count
from {{ ref('dim_user') }}
where is_current
group by user_id_hash
having count(*) > 1
