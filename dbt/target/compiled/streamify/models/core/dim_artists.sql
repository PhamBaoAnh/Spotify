

SELECT lower(hex(MD5(toString(coalesce(cast(artistId as String), '_dbt_utils_surrogate_key_null_') )))) AS artistKey,
    *
FROM (
        SELECT 
            MAX(artist_id) AS artistId,
            MAX(artist_latitude) AS latitude,
            MAX(artist_longitude) AS longitude,
            MAX(artist_location) AS location,
            REPLACE(REPLACE(artist_name, '"', ''), '\\', '') AS name
        FROM `default`.`songs_staging`
        GROUP BY artist_name

        UNION ALL

        SELECT 'NNNNNNNNNNNNNNN',
            0,
            0,
            'NA',
            'NA'
    )