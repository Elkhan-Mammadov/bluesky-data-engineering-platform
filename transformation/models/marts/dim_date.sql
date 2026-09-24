-- Standard date dimension. Built with generate_series rather than a
-- dbt package, to keep the project dependency-free (no `dbt deps` step,
-- no internet access needed to build this model).
with days as (
    select generate_series(
        '2024-01-01'::date,
        '2030-12-31'::date,
        interval '1 day'
    )::date as date_day
)
select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day_of_month,
    extract(isodow from date_day)::int as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    trim(to_char(date_day, 'Month')) as month_name
from days
