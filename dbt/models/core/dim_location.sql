{{ config(materialized='table') }}

WITH location_data AS (
    SELECT DISTINCT
        le.city,
        ifNull(sc.stateCode, 'NA') AS stateCode,
        ifNull(sc.stateName, 'NA') AS stateName,
        le.lat AS latitude,
        le.lon AS longitude
    FROM {{ source('staging', 'listen_events_staging') }} AS le
    LEFT JOIN {{ source('staging', 'state_codes_staging') }} AS sc
        ON le.state = sc.stateCode

    UNION ALL

    SELECT 
        'NA' AS city,
        'NA' AS stateCode,
        'NA' AS stateName,
        0.0 AS latitude,
        0.0 AS longitude
)

SELECT
    cityHash64(concat(city, stateName, toString(latitude), toString(longitude))) AS locationKey,
    city,
    stateCode,
    stateName,
    latitude,
    longitude
FROM location_data
