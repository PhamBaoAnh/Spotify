

SELECT 
    cityHash64(toString(songId)) AS songKey,
    songId,
    artistName,
    duration,
    key,
    keyConfidence,
    loudness,
    songHotness,
    tempo,
    title,
    year
FROM
(
    SELECT 
        song_id AS songId,
        REPLACE(REPLACE(artist_name, '"', ''), '\\', '') AS artistName,
        CAST(duration AS Float64) AS duration,
        CAST(key AS Int32) AS key,
        CAST(key_confidence AS Float64) AS keyConfidence,
        CAST(loudness AS Float64) AS loudness,
        CAST(song_hotttnesss AS Float64) AS songHotness,
        CAST(tempo AS Float64) AS tempo,
        title,
        CAST(year AS Int32) AS year
    FROM `default`.`songs_staging`

    UNION ALL

    SELECT
        'NNNNNNNNNNNNNNNNNNN' AS songId,
        'NA' AS artistName,
        0.0 AS duration,
        -1 AS key,
        -1.0 AS keyConfidence,
        -1.0 AS loudness,
        -1.0 AS songHotness,
        -1.0 AS tempo,
        'NA' AS title,
        0 AS year
)