SELECT
 context_app_name
,data_event
,CAST(created_at AS DATE) AS created_at
,CAST(sent_at AS DATE) AS sent_at
,case when context_traits_user_id is null then data_properties_auth0_id else context_traits_user_id end as auth0_id
,data_properties_asset_name
,data_properties_author_name
,data_properties_background_name
,data_properties_content_name
,data_properties_country
,data_properties_day_part
,data_properties_duration
,data_properties_feeling_type
,data_properties_iOS
,case when data_properties_outcome is null then data_properties_Outcome_name else data_properties_outcome end as data_properties_outcome
,data_properties_platform
,data_properties_rating
,data_properties_series_name

FROM `mindvalley-event-stream`.events.omvana_v2