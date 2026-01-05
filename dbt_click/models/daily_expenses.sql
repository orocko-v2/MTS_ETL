WITH expenses AS (
    SELECT
        phone,
         sumIf(amount, operation_type != 'income') as total_expense
    from {{source('mtsetl', 'operations')}} final
    where toDate(datetime) = toDate(now()) - INTERVAL 1 DAY
    group by phone
)

SELECT
    expenses.phone,
    workers.worker_name,
    expenses.total_expense
FROM expenses
JOIN {{source('mtsetl','workers')}} workers ON expenses.phone=workers.phone
ORDER BY expenses.phone