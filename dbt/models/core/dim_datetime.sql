{{ config(materialized = 'table') }}

SELECT
    toUnixTimestamp(date) AS dateKey,
    date,
    toDayOfWeek(date) AS dayOfWeek,
    toDayOfMonth(date) AS dayOfMonth,
    toWeek(date) AS weekOfYear,
    toMonth(date) AS month,
    toYear(date) AS year,
    if(toDayOfWeek(date) IN (6,7), 1, 0) AS weekendFlag
FROM 
(
    SELECT arrayJoin(
        arrayMap(
            x -> toDateTime('2025-08-25 00:00:00') + x,
            range(
                toUInt32(
                    toUnixTimestamp(toDateTime('2025-08-26 23:59:59')) - toUnixTimestamp(toDateTime('2025-08-25 00:00:00')) + 1
                )
            )
        )
    ) AS date
)
