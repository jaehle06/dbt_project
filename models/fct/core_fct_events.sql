{{ 
  config(
    materialized = 'incremental',
    unique_key = 'user_pseudo_id'
  )
}}

SELECT  
  distinct
  PARSE_DATE('%Y%m%d', event_date) AS date,
  DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS datetime,
  COALESCE(user_pseudo_id, 'UNK') AS user_pseudo_id,
  COALESCE((SELECT value.int_value FROM UNNEST (event_params) WHERE key = 'ga_session_id'), -1) AS ga_session_id,
  COALESCE((SELECT value.int_value FROM UNNEST (event_params) WHERE key = 'ga_session_number'), -1) AS ga_session_number,
  event_name,
  COALESCE(device.web_info.hostname, 'UNK') AS hostname,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'page_location'), 'UNK') AS page_location_full,        
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'page_title'), 'UNK') AS page_title,
  COALESCE(traffic_source.name, 'UNK') AS first_utm_campaign,
  COALESCE(traffic_source.medium, 'UNK') AS first_utm_medium,
  COALESCE(traffic_source.source, 'UNK') AS first_utm_source,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'campaign_id'), 'UNK') AS event_utm_campaign_id,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'campaign'), 'UNK') AS event_utm_campaign,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'medium'), 'UNK') AS event_utm_medium,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'source'), 'UNK') AS event_utm_source,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'term'), 'UNK') AS event_utm_term,
  COALESCE((SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'content'), 'UNK') AS event_utm_content,
 FROM {{ ref('src_ga4') }} 
 
where (1=1)
  {% if is_incremental() %}
    AND PARSE_DATE('%Y%m%d', event_date) >= (select max(date) from {{ this }})
  {% endif %}
AND event_date is not null
AND event_timestamp is not null
AND event_name is not null


