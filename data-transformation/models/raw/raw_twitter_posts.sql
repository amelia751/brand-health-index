{{ config(
    materialized='view',
    description='Raw Twitter posts data from Fivetran ingestion'
) }}

select
    brand_id,
    tweet_id,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event) as ts_event,
    date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) as date,
    author_id,
    author_username,
    author_verified,
    author_location,
    text,
    lang,
    like_count,
    reply_count,
    retweet_count,
    quote_count,
    possibly_sensitive,
    geo_country,
    geo_place_id,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', collected_at) as collected_at,
    
    -- Add derived fields
    (like_count + reply_count + retweet_count + quote_count) as total_engagements,
    length(text) as text_length

from {{ source('fivetran_raw', 'twitter_posts') }}

where 
    -- Data quality filters
    tweet_id is not null
    and brand_id is not null
    and text is not null
    and text != ''
    
    -- Date range filter
    {% if var('start_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) >= '{{ var("start_date") }}'
    {% endif %}
    
    {% if var('end_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) <= '{{ var("end_date") }}'
    {% endif %}
