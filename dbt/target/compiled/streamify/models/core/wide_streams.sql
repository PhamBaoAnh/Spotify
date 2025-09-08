

SELECT
    f.ts AS ts,
    f.userKey AS userKey,
    f.artistKey AS artistKey,
    f.songKey AS songKey,
    f.dateKey AS dateKey,
    f.locationKey AS locationKey,
    f.ts AS timestamp,
    
    u.firstName AS firstName,
    u.lastName AS lastName,
    u.gender AS gender,
    u.level AS level,
    u.userId AS userId,
    u.currentRow AS currentUserRow,

    s.duration AS songDuration,
    s.tempo AS tempo,
    s.title AS songName,

    l.city AS city,
    l.stateName AS state,
    l.latitude AS latitude,
    l.longitude AS longitude,

    d.date AS dateHour,
    d.dayOfMonth AS dayOfMonth,
    d.dayOfWeek AS dayOfWeek,

    a.latitude AS artistLatitude,
    a.longitude AS artistLongitude,
    a.name AS artistName

FROM `streamify_prod`.`fact_streams` AS f
INNER JOIN `streamify_prod`.`dim_users` AS u ON f.userKey = u.userKey
INNER JOIN `streamify_prod`.`dim_songs` AS s ON f.songKey = s.songKey
INNER JOIN `streamify_prod`.`dim_location` AS l ON f.locationKey = l.locationKey
INNER JOIN `streamify_prod`.`dim_datetime` AS d ON f.dateKey = d.dateKey
INNER JOIN `streamify_prod`.`dim_artists` AS a ON f.artistKey = a.artistKey