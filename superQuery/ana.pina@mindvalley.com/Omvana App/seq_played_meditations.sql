with raw_data as (

SELECT
distinct
 context_app_name
,data_event
,CAST(created_at AS DATE) AS event_date
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

where  (data_event="play background" or data_event="play media" or data_event="play asset" or data_event="play class content" ) )

,meditations_sequence as (
SELECT
event_date
,auth0_id
,case when row_number() over (partition by auth0_id order by event_date asc) > 2 then null else row_number() over (partition by auth0_id order by event_date asc) end as meditations_seq
,case when row_number() over (partition by auth0_id order by event_date asc) > 2 then null else data_properties_asset_name end as data_properties_asset_name

from raw_data where auth0_id is not null

)

,final_sequence_by_user as (
select
auth0_id
,trim(string_agg(data_properties_asset_name order by meditations_seq)) as sequence
from meditations_sequence

group by auth0_id)

select
sequence, count(*) as users

from final_sequence_by_user

group by sequence

order by users desc