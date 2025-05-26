{{ config(
    materialized='incremental',
    unique_key='concat(user_pseudo_id, "-", cast(ga_session_id as string))',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns'
) }}
SELECT
PARSE_DATE('%Y%m%d', event_date) AS date,
*
FROM `practice-215909.analytics_336023799.events_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))