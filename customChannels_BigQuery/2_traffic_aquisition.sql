/*  
Original Author: Krisjan Oldekamp
https://stacktonic.com/article/google-analytics-4-and-big-query-create-custom-channel-groupings-in-a-reusable-sql-function   
Modified by: Leandro Nascimento to use BigQuery sample dataset for Google Analytics 4 ecommerce web implementation (https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset)

Note, Krisjan Oldekamp's original solution is created thinking on current, up-to-date, bigquery datsets. However the BigQuery sample dataset for Google Analytics 4 is not. It's latest date is 2021-01-31.
So, for that we are declaring a variable with the 2021-01-31 date in place of the current date. You can easily change this by swapping the commenting (three dashes) between the declared variables
*/

--- DECLARE d date default current_date();
DECLARE d date default "2021-01-31";


with
sessions as (
/*
This is the first step of this solution: 
 1- we are creating the session, grouping all the session events with the visitor ID (user_pseudo_id) and a session ID (concated (user_pseudo_id and ga_session_id)
 2- aggregating all pageview, 'user_engagement','scroll' events within this session with the information of source, medium & campaign (plus if it is the entrance event and if it has ignore_referrer)
 3- count conversions and add up their value for the entire session.
*/
    select
        user_pseudo_id as ga_client_id, 
        concat(user_pseudo_id,'.',(select cast(value.int_value as string) from unnest(event_params) where key = 'ga_session_id')) as session_id, -- combine user_pseudo_id and session_id for a unique session-id
        timestamp_micros(min(event_timestamp)) as session_start,
        array_agg(
            if(event_name in('page_view','user_engagement','scroll'), struct(
                event_timestamp,
                lower((select value.string_value from unnest(event_params) where key = 'source')) as source,
                lower((select value.string_value from unnest(event_params) where key = 'medium')) as medium,
                lower((select value.string_value from unnest(event_params) where key = 'name')) as name,
                lower((select value.string_value from unnest(event_params) where key = 'campaign')) as campaign,
                (select value.int_value from unnest(event_params) where key = 'entrances') as is_entrance,
                (select value.int_value from unnest(event_params) where key = 'ignore_referrer') as ignore_referrer
            ), null) 
        ignore nulls) as channels_in_session,
        countif(event_name = 'purchase') as conversions,
        sum(ecommerce.purchase_revenue) as conversion_value
    from
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
    where
        _table_suffix between 
            format_date('%Y%m%d', date_sub(d, interval 30 day))
            and format_date('%Y%m%d', date_sub(d, interval 1 day))
    group by
        user_pseudo_id,
        session_id
),


traffic_acquisition as (
/*
This is the second step of this solution: 
 1- We are using the information from the sessions query to define which are the source, medium and campaign from that specific session. 
    Basically this will create GA4's default channel grouping (following the logic described here https://support.google.com/analytics/answer/9756891?hl=en)
 2- We count the sessions and add up conversions and conversion value.
*/
    select
        (select t.source from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as source,
        (select t.medium from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as medium,
        (select t.campaign from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as campaign,
        (select t.name from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as name,
        count(distinct session_id) as sessions,
        sum(conversions) as conversions,
        ifnull(sum(conversion_value), 0) as conversion_value
    from
        sessions
    group by
        1, 2, 3, 4
)

-- map source / medium to channel grouping using a user defined function (ignore campaign)
select
    *

from
    traffic_acquisition
