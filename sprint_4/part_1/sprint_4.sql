--Задание 1

SELECT order_dt
FROM orders
WHERE order_id = 153;

SELECT order_id
FROM orders
WHERE order_dt > current_date::timestamp;

SELECT count(*)
FROM orders
WHERE user_id = '329551a1-215d-43e6-baee-322f2467272d';

Есть несколько индексов на таблице, и они могут замедлять операции вставки, особенно если таблица имеет большой размер. 
Можно временно отключить некоторые индексы и проверить, улучшится ли производительность вставки.
Использование MAX(order_id) + 1 может вызывать блокировки. Можно использовать последовательности для генерации уникальных идентификаторов.
Некоторые из индексов могут быть избыточными, и их можно удалить без значительного воздействия на производительность запросов.
Нужно проверить, актуальны ли статистические данные для таблицы. Выполните запрос ANALYZE orders; для обновления статистики.
Так же стоит проверить, достаточно ли ресурсов на сервере для обработки операций вставки. 
Возможно, увеличение выделенных ресурсов (памяти, CPU) поможет ускорить операции.
Триггеры и правила: Проверьте, есть ли на таблице триггеры или правила, которые могут замедлять операции вставки.

-- Создание последовательности
CREATE SEQUENCE orders_order_id_seq;

-- Вставка данных с использованием последовательности
INSERT INTO orders (order_id, order_dt, user_id, device_type, city_id, total_cost, discount, final_cost)
VALUES (nextval('orders_order_id_seq'), current_timestamp, '329551a1-215d-43e6-baee-322f2467272d', 'Mobile', 1, 1000.00, null, 1000.00);

SELECT
  indexrelname AS index_name,
  idx_scan AS index_scans,
  idx_tup_read AS tuples_read,
  idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE relname = 'orders';


По результатам мониторинга использования индексов, некоторые индексы имеют нулевые значения в столбце index_scans.А именно индексы orders_city_id_idx, 
orders_device_type_city_id_idx, orders_device_type_idx, orders_discount_idx, orders_final_cost_idx, orders_total_cost_idx, и orders_total_final_cost_discount_idx не были задействованы в последних запросах

-- Удаление неиспользуемых индексов
DROP INDEX IF EXISTS orders_city_id_idx;
DROP INDEX IF EXISTS orders_device_type_city_id_idx;
DROP INDEX IF EXISTS orders_device_type_idx;
DROP INDEX IF EXISTS orders_discount_idx;
DROP INDEX IF EXISTS orders_final_cost_idx;
DROP INDEX IF EXISTS orders_total_cost_idx;
DROP INDEX IF EXISTS orders_total_final_cost_discount_idx;


--Задание 2

SELECT user_id::text::uuid, first_name::text, last_name::text, 
    city_id::bigint, gender::text
FROM users
WHERE city_id::integer = 4
    AND date_part('day', to_date(birth_date::text, 'yyyy-mm-dd')) 
        = date_part('day', to_date('31-12-2023', 'dd-mm-yyyy'))
    AND date_part('month', to_date(birth_date::text, 'yyyy-mm-dd')) 
        = date_part('month', to_date('31-12-2023', 'dd-mm-yyyy')) 
        

Вместо преобразования типов каждый раз в условиях WHERE, можно сделать преобразование один раз перед выполнением основного запроса.
Так же использовать EXTRACT вместо date_part

 WITH birthday_users AS (
    SELECT 
        user_id::text::uuid,
        first_name::text,
        last_name::text,
        city_id::bigint,
        gender::text
    FROM users
    WHERE city_id::integer = 4
        AND EXTRACT(day FROM to_date(birth_date, 'yyyy-mm-dd')) = 31
        AND EXTRACT(month FROM to_date(birth_date, 'yyyy-mm-dd')) = 12
)

SELECT * FROM birthday_users;

Создать индексы на столбцах, используемых в условиях WHERE (city_id, birth_date), если их нет

CREATE INDEX ON users(city_id);
CREATE INDEX ON users(birth_date);



--Задание 3

SELECT proname, prosrc
FROM pg_proc
WHERE proname = 'add_payment';

BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());
    
    INSERT INTO payments (payment_id, order_id, payment_sum)
    VALUES (nextval('payments_payment_id_sq'), p_order_id, p_sum_payment);
    
    INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    SELECT NEXTVAL('sales_sale_id_sq'), statement_timestamp(), user_id, p_sum_payment
    FROM orders WHERE order_id = p_order_id;
END;

Для оптимизации процедуры add_payment можно убрать  INSERT INTO payments , так как в нем дублируются данные

DO $$ 
BEGIN
    -- Ваш код начинается здесь
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());
    
    INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    SELECT NEXTVAL('sales_sale_id_sq'), statement_timestamp(), user_id, p_sum_payment
    FROM orders WHERE order_id = p_order_id;

END $$;


--Задание 4

Ускорение записи данных в таблицу user_logs можно осуществить несколькими способами:

Проверить, есть ли у таблицы подходящие индексы. Если анализирются данные за текущий квартал, то добавление индекса по времени записи может существенно ускорить выборку.

CREATE INDEX idx_user_logs_timestamp ON user_logs(timestamp);


Вместо того, чтобы вставлять записи по одной,  можно использовать оператор INSERT INTO ... VALUES ... с несколькими парами значений для более эффективной вставки. Это может снизить накладные расходы на коммит и улучшить производительность.


INSERT INTO user_logs (timestamp, user_id, action)
VALUES 
('2024-01-01 12:00:00', 123, 'login'),
('2024-01-01 12:15:00', 456, 'logout'),
...;
Так же важно убедиться, что параметры конфигурации PostgreSQL настроены оптимальным образом для вашего оборудования и рабочей нагрузки. 
Например, параметры, такие как shared_buffers, work_mem, и wal_level, могут оказать влияние на производительность записи.
Можно рассмотреть возможность разделения таблицы user_logs на части (по кварталам, например) с использованием разделения таблиц. 
Это может снизить накладные расходы на управление индексами и ускорить выполнение запросов в зависимости от времени.
Прежде чем вносить изменения в производственную базу данных, рекомендуется провести тестирование на копии базы данных и оценить влияние каждого изменения на производительность.





--Задание 5

Для оптимальной производительности и уменьшения нагрузки на базу данных, можно использовать материализованные представления для предварительного вычисления статистики, 
которая будет использоваться в отчётах. В данном случае, мы можем создать материализованное представление, которое будет хранить статистику по предпочтениям пользователей по возрастным группам и блюдам.

CREATE MATERIALIZED VIEW age_group_preferences AS
SELECT
  age_group,
  AVG(spicy) AS avg_spicy,
  AVG(fish) AS avg_fish,
  AVG(meat) AS avg_meat
FROM (
  SELECT
    CASE
      WHEN EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) >= 0 AND EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) <= 20 THEN '0–20'
      WHEN EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) > 20 AND EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) <= 30 THEN '20–30'
      WHEN EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) > 30 AND EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) <= 40 THEN '30–40'
      WHEN EXTRACT(YEAR FROM AGE(current_date, CAST(u.birth_date AS date))) > 40 THEN '40–100'
    END AS age_group,
    d.spicy,
    d.fish,
    d.meat
  FROM public.dishes d
  JOIN order_items oi ON d.object_id = oi.item
  JOIN orders o ON oi.order_id = o.order_id
  JOIN public.users u ON o.user_id::text = u.user_id
  WHERE CAST(u.birth_date AS date) < current_date
) AS user_preferences
GROUP BY age_group;

SELECT * FROM age_group_preferences;








