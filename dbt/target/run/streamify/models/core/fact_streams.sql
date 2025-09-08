
  
    
    
    

    
        create table `streamify_prod`.`fact_streams`
        
  
        
  engine = MergeTree()
        order by (ts)
        
        
        
          as (
            

WITH fact AS (
    SELECT
        ts,
        toStartOfHour(ts) AS ts_hour,
        userId,
        replaceAll(replaceAll(artist, '"', ''), '\\', '') AS clean_artist,
        song,
        city,
        state AS stateCode,
        lat AS latitude,
        lon AS longitude,
        toDate(ts) AS ts_date
    FROM `default`.`listen_events_staging`
)

SELECT
    u.userKey AS userKey,
    a.artistKey AS artistKey,
    s.songKey AS songKey,
    d.dateKey AS dateKey,
    l.locationKey AS locationKey,
    f.ts AS ts
FROM fact f
LEFT JOIN `streamify_prod`.`dim_users` u
    ON f.userId = u.userId
LEFT JOIN `streamify_prod`.`dim_artists` a
    ON f.clean_artist = a.name
LEFT JOIN `streamify_prod`.`dim_songs` s
    ON f.clean_artist = s.artistName
    AND f.song = s.title
LEFT JOIN `streamify_prod`.`dim_location` l
    ON f.city = l.city
    AND f.stateCode = l.stateCode
    AND f.latitude = l.latitude
    AND f.longitude = l.longitude
LEFT JOIN `streamify_prod`.`dim_datetime` d
    ON toDate(d.date) = f.ts_date
WHERE u.userKey IS NULL
   OR (f.ts >= u.rowActivationDate AND f.ts <= coalesce(u.rowExpirationDate, now()))
          )
  