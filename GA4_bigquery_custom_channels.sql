/*  
Original Author: Krisjan Oldekamp
https://stacktonic.com/article/google-analytics-4-and-big-query-create-custom-channel-groupings-in-a-reusable-sql-function   
Modified by: Leandro Nascimento to use BigQuery sample dataset for Google Analytics 4 ecommerce web implementation (https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset)

 This is the first step of this solution: 
 1- we are creating the session, grouping all the session events with the visitor ID (user_pseudo_id) and a session ID (concated (user_pseudo_id and ga_session_id)
 2- aggregating all pageview, 'user_engagement','scroll' events within this session with the information of source, medium & campaign (plus if it is the entrance event and if it has ignore_referrer)
 3- count conversions and add up their value for the entire session.
 
 Note, Krisjan Oldekamp's original solution is created thinking on current, up-to-date, bigquery datsets. However the BigQuery sample dataset for Google Analytics 4 is not. It's latest date is 2021-01-31.
 So, for that we are declaring a variable with the 2021-01-31 date in place of the current date. You can easily change this by swapping the commenting (three dashes) between the declared variables
*/

--- DECLARE d date default current_date();
DECLARE d date default "2021-01-31";

    select
        user_pseudo_id as ga_client_id, 
        concat(user_pseudo_id,'.',(select cast(value.int_value as string) from unnest(event_params) where key = 'ga_session_id')) as session_id, -- combine user_pseudo_id and session_id for a unique session-id
        timestamp_micros(min(event_timestamp)) as session_start,
        array_agg(
            if(event_name in('page_view','user_engagement','scroll'), struct(
                event_timestamp,
                lower((select value.string_value from unnest(event_params) where key = 'source')) as source,
                lower((select value.string_value from unnest(event_params) where key = 'medium')) as medium,
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
