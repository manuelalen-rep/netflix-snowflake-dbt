WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'netflix_raw') }}
)

SELECT
    show_id,
    type,
    title,
    CASE 
        WHEN duration ILIKE '%min%' THEN TRY_CAST(REGEXP_SUBSTR(duration, '(\\d+)') AS INT)
        ELSE NULL 
    END as duration_minutes,
    
    CASE 
        WHEN duration ILIKE '%Season%' THEN TRY_CAST(REGEXP_SUBSTR(duration, '(\\d+)') AS INT)
        ELSE NULL 
    END as duration_seasons,
    COALESCE(
        TRY_TO_DATE(TRIM(date_added, ' "'), 'MMMM DD, YYYY'),
        TRY_TO_DATE(TRIM(date_added, ' "'), 'MON DD, YYYY'),
        TRY_TO_DATE(TRIM(date_added, ' "')) 
    ) as date_added_clean,
    COALESCE(DURATION_MINUTES, DURATION_SEASONS) as durantion_minutes_season,
    release_year,
    rating,
    listed_in as categories

FROM raw_data