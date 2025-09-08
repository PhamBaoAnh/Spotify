{{ config(materialized='table') }}

-- 1️⃣ Base: lấy dữ liệu user bình thường
WITH base AS (
    SELECT
        CAST(userId AS Int64) AS userId,
        firstName,
        lastName,
        gender,
        CAST(registration AS Int64) AS registration,
        level,
        CAST(ts AS DateTime) AS rowActivationDate
    FROM {{ source('staging', 'listen_events_staging') }}
    WHERE userId NOT IN (0,1)
),

-- 2️⃣ Tính ngày bắt đầu mỗi level
activation AS (
    SELECT
        userId, firstName, lastName, gender, registration, level,
        min(rowActivationDate) AS rowActivationDate
    FROM base
    GROUP BY userId, firstName, lastName, gender, registration, level
),

-- 3️⃣ Tạo array chứa ngày bắt đầu các level kế tiếp
arrays AS (
    SELECT
        a.userId,
        a.firstName,
        a.lastName,
        a.gender,
        a.registration,
        a.level,
        a.rowActivationDate,
        arrayConcat(
            arrayMap(x -> CAST(x AS DateTime),
                     arrayFilter(x -> x > a.rowActivationDate, groupArray(b.rowActivationDate))),
            [toDateTime('2030-12-31 23:59:59')]
        ) AS nextDates
    FROM activation a
    ANY JOIN activation b
        ON a.userId = b.userId
    GROUP BY a.userId, a.firstName, a.lastName, a.gender, a.registration, a.level, a.rowActivationDate
),

-- 4️⃣ Tạo bản ghi SCD2
scd2 AS (
    SELECT
        userId,
        firstName,
        lastName,
        gender,
        registration,
        level,
        rowActivationDate,
        arrayElement(nextDates, 1) AS rowExpirationDate,
        CASE WHEN arrayElement(nextDates, 1) = toDateTime('2030-12-31 23:59:59') THEN 1 ELSE 0 END AS currentRow
    FROM arrays
),

static_users AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['userId', 'level']) }} AS userKey,
        CAST(userId AS Int64) AS userId,
        firstName,
        lastName,
        gender,
        CAST(registration AS Int64) AS registration,
        level,
        toDateTime('2023-01-01 00:00:00') AS rowActivationDate,
        toDateTime('2030-12-31 23:59:59') AS rowExpirationDate,
        1 AS currentRow
    FROM {{ source('staging', 'listen_events_staging') }}
    WHERE userId IN (0,1)
    GROUP BY userId, firstName, lastName, gender, level, registration
)

-- 6️⃣ Kết hợp tất cả
SELECT
    {{ dbt_utils.generate_surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey,
    userId, firstName, lastName, gender, registration, level,
    rowActivationDate, rowExpirationDate, currentRow
FROM scd2

UNION ALL

SELECT * FROM static_users
