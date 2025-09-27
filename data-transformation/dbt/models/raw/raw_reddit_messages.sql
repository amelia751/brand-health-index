{{ config(
    materialized='view',
    description='Raw Reddit posts and comments data from Fivetran ingestion'
) }}

select
    brand_id,
    reddit_id,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event) as ts_event,
    date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) as date,
    subreddit,
    author,
    type,
    title,
    body,
    score,
    num_comments,
    upvote_ratio,
    url,
    permalink,
    lang,
    geo_country,
    parse_timestamp('%Y-%m-%dT%H:%M:%E*S', collected_at) as collected_at,
    
    -- Add derived fields
    case 
        when body is not null then length(body)
        when title is not null then length(title)
        else 0
    end as text_length,
    
    coalesce(body, title, '') as combined_text

from {{ source('fivetran_raw', 'reddit_messages') }}

where 
    -- Data quality filters
    reddit_id is not null
    and brand_id is not null
    and (body is not null or title is not null)
    and author != '[deleted]'
    
    -- Date range filter
    {% if var('start_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) >= '{{ var("start_date") }}'
    {% endif %}
    
    {% if var('end_date') %}
    and date(parse_timestamp('%Y-%m-%dT%H:%M:%E*S', ts_event)) <= '{{ var("end_date") }}'
    {% endif %}
