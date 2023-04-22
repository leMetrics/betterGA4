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

declare d date default "2021-01-31";
declare conversion_period int64 default 90; -- select conversions in last x days
-- declare lookback_window int64 default 30; -- how many days to lookback from the moment the conversion occurred;

with
-- group event level google analytics 4 data to sessions (visits)
sessions as (
    select
        user_pseudo_id as ga_client_id,
        --user_id as custom_user_id, -- use a custom user-id instead, like a customer-id
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
        --countif(event_name = '<name-of-some-other-conversion-event>') as conversions_in_session,
        countif(event_name = 'purchase') as conversions,
        sum(ecommerce.purchase_revenue) as conversion_value
    from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    where
        -- select conversions based on <conversion_period> + additional daterange to construct the path of a conversion (based on <lookback_window>)
        _table_suffix between 
            format_date('%Y%m%d', date_sub(d, interval conversion_period day))
            and format_date('%Y%m%d', date_sub(d, interval 1 day))
		
--		_table_suffix between 
--            format_date('%Y%m%d', date_sub(current_date(), interval (conversion_period + lookback_window) day))
--            and format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
    group by 1, 2
),
-- build conversion paths for all sessions with at least 1 conversion within the last <conversion_period> days
sessions_converted as (
    select
        s.session_start,
        s.session_id,
        string_agg(
            -- select first channel / campaign within session
            `<your-project>.<your-dataset>.channel_grouping`(
                (select t.source from unnest(s_lb.channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1),
                (select t.medium from unnest(s_lb.channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1),
                null
            ),
            ' > '
            order by s_lb.session_start asc
        ) as path_channels,
        string_agg(cast(timestamp_diff(timestamp(s.session_start), timestamp(s_lb.session_start), hour) as string), ' > ' order by s_lb.session_start asc) as path_timestamps, -- hours till conversion
        string_agg(cast(s_lb.session_start as string), ' > ' order by s_lb.session_start asc) as path_timestamps_check,
        max(s.conversions) as conversions_in_session,
        max(s.conversion_value) as conversion_value
    from sessions as s
    left join
        -- joining historical sessions to construct the conversion path (with a max path length of <lookback_window>)
        sessions as s_lb
        on s.ga_client_id = s_lb.ga_client_id
        and s.session_start >= s_lb.session_start -- only join current session and sessions before current session
        and datetime(s_lb.session_start) >= date_sub(datetime(s.session_start), interval lookback_window day) -- only join sessions not older than <lookback_window> days counted from conversion
    where
        s.conversions > 0
        and date(s.session_start) >= date_sub(current_date(), interval conversion_period day)
    group by 1, 2
    order by 
        s.session_start asc
)

-- query data on user (journey) level
select
    date(session_start) as conversion_date,
    session_start as conversion_timestamp,
    session_id as journey_id,
    path_channels,
    path_timestamps,
    true as conversion,
    conversions_in_session,
    conversion_value as conversion_value
from sessions_converted
