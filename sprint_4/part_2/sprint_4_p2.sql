SELECT pg_stat_statements_reset();

-- Выбираем 5 самых медленных скриптов
SELECT queryid, query, total_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;


-
-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );
   

Explain ANALYZE:

Nested Loop  (cost=15.51..33173.85 rows=44 width=54) (actual time=220.842..220.850 rows=2 loops=1)
  Join Filter: (os.status_id = s.status_id)
  Rows Removed by Join Filter: 10
  ->  Seq Scan on statuses s  (cost=0.00..22.70 rows=1270 width=36) (actual time=2.004..2.006 rows=6 loops=1)
  ->  Materialize  (cost=15.51..33017.82 rows=7 width=26) (actual time=29.332..36.468 rows=2 loops=6)
        ->  Hash Join  (cost=15.51..33017.78 rows=7 width=26) (actual time=175.841..218.655 rows=2 loops=1)
              Hash Cond: (os.order_id = o.order_id)
              Join Filter: (SubPlan 1)
              Rows Removed by Join Filter: 10
              ->  Seq Scan on order_statuses os  (cost=0.00..2035.34 rows=124334 width=20) (actual time=2.949..60.198 rows=124334 loops=1)
              ->  Hash  (cost=15.47..15.47 rows=3 width=22) (actual time=14.295..14.295 rows=2 loops=1)
                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
                    ->  Bitmap Heap Scan on orders o  (cost=4.31..15.47 rows=3 width=22) (actual time=13.163..13.173 rows=2 loops=1)
                          Recheck Cond: (user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid)
                          Heap Blocks: exact=1
                          ->  Bitmap Index Scan on orders_user_id_idx  (cost=0.00..4.31 rows=3 width=0) (actual time=12.463..12.463 rows=2 loops=1)
                                Index Cond: (user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid)
              SubPlan 1
                ->  Aggregate  (cost=2346.19..2346.20 rows=1 width=8) (actual time=8.842..8.842 rows=1 loops=12)
                      ->  Seq Scan on order_statuses  (cost=0.00..2346.18 rows=5 width=8) (actual time=8.800..8.835 rows=6 loops=12)
                            Filter: (order_id = o.order_id)
                            Rows Removed by Filter: 124328
Planning Time: 14.441 ms
Execution Time: 224.693 ms


-- Исправленный индекс
CREATE INDEX idx_order_statuses_order_id_status_id ON public.order_statuses(order_id, status_id, status_dt DESC);

-- Оптимизированный запрос
SELECT  
    o.order_id,  
    o.order_dt,  
    o.final_cost,  
    s.status_name 
FROM  
    orders o 
JOIN LATERAL (
    SELECT
        os.order_id,
        os.status_id,
        os.status_dt,
        ROW_NUMBER() OVER (PARTITION BY os.order_id ORDER BY os.status_dt DESC) AS rn
    FROM  
        order_statuses os 
    WHERE  
        os.order_id = o.order_id 
) os ON true 
JOIN statuses s ON os.status_id = s.status_id
WHERE  
    o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
    AND os.rn = 1;


QUERY PLAN
Hash Join  (cost=29.71..60.54 rows=19 width=54) (actual time=0.134..0.139 rows=2 loops=1)
  Hash Cond: (s.status_id = os.status_id)
  ->  Seq Scan on statuses s  (cost=0.00..22.70 rows=1270 width=36) (actual time=0.009..0.010 rows=6 loops=1)
  ->  Hash  (cost=29.68..29.68 rows=3 width=26) (actual time=0.109..0.110 rows=2 loops=1)
        Buckets: 1024  Batches: 1  Memory Usage: 9kB
        ->  Nested Loop  (cost=8.87..29.68 rows=3 width=26) (actual time=0.072..0.100 rows=2 loops=1)
              ->  Bitmap Heap Scan on orders o  (cost=4.31..15.47 rows=3 width=22) (actual time=0.017..0.018 rows=2 loops=1)
                    Recheck Cond: (user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid)
                    Heap Blocks: exact=1
                    ->  Bitmap Index Scan on orders_user_id_idx  (cost=0.00..4.31 rows=3 width=0) (actual time=0.014..0.014 rows=2 loops=1)
                          Index Cond: (user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid)
              ->  Subquery Scan on os  (cost=4.56..4.73 rows=1 width=4) (actual time=0.031..0.038 rows=1 loops=2)
                    Filter: (os.rn = 1)
                    Rows Removed by Filter: 5
                    ->  WindowAgg  (cost=4.56..4.66 rows=5 width=28) (actual time=0.030..0.036 rows=6 loops=2)
                          ->  Sort  (cost=4.56..4.58 rows=5 width=20) (actual time=0.023..0.024 rows=6 loops=2)
                                Sort Key: os_1.status_dt DESC
                                Sort Method: quicksort  Memory: 25kB
                                ->  Index Only Scan using idx_order_statuses_order_id_status_id on order_statuses os_1  (cost=0.42..4.50 rows=5 width=20) (actual time=0.011..0.013 rows=6 loops=2)
                                      Index Cond: (order_id = o.order_id)
                                      Heap Fetches: 0
Planning Time: 0.376 ms
Execution Time: 0.232 ms

Общее время выполнения исходного запроса до оптимизации: 224.693 ms
Общее время выполнения оптимизированного запроса: 0.232 ms

Узлы с с высокой стоимостью :
Hash Join (cost=15.51..33017.78 rows=7 width=26)
Seq Scan on order_statuses os (cost=0.00..2035.34 rows=124334 width=20)
Aggregate (cost=2346.19..2346.20 rows=1 width=8)

Оптимизация включала следующие изменения:

Замена подзапроса с использованием IN на более эффективный вариант с использованием LATERAL JOIN и группировкой внутри подзапроса.
Добавление индекса idx_order_statuses_order_id_status_id на order_statuses для улучшения производительности.


В этом запросе использована конструкция LATERAL, которая выполняет подзапрос для каждой строки основной таблицы orders. 
Это позволяет избежать использования материализации и, возможно, улучшит производительность. 
Стало эффективное использование индекса orders_user_id_idx, и запрос быстро находит нужные строки по user_id.
Hash Join с таблицей statuses: Здесь происходит объединение результата с таблицей statuses. Так как это небольшая таблица , Seq Scan хорошо подходит.
Hash Join с подзапросом os: В этом случае, подзапрос создает временную таблицу, а затем происходит хеш-объединение. Подзапрос эффективно использует индекс order_statuses(order_id).
План выполнения Subquery Scan: Подзапрос сначала сортирует строки, затем выполняет группировку и агрегацию. 

Так же был добавлен мультиколоночный индекс на таблицу order_statuses. 
Данная оптимизация поможет снизить нагрузку на сортировку в подзапросе и улучшить производительность. 


-- 7
-- ищет действия и время действия определенного посетителя

SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;


Explain ANALYZE:
QUERY PLAN
Gather Merge  (cost=92015.55..92039.11 rows=202 width=19) (actual time=1054.583..1066.194 rows=10 loops=1)
  Workers Planned: 2
  Workers Launched: 2
  ->  Sort  (cost=91015.52..91015.78 rows=101 width=19) (actual time=929.874..930.127 rows=3 loops=3)
        Sort Key: user_logs.datetime
        Sort Method: quicksort  Memory: 25kB
        Worker 0:  Sort Method: quicksort  Memory: 25kB
        Worker 1:  Sort Method: quicksort  Memory: 25kB
        ->  Parallel Append  (cost=0.00..91012.16 rows=101 width=19) (actual time=165.788..926.973 rows=3 loops=3)
              ->  Parallel Seq Scan on user_logs_y2021q2 user_logs_2  (cost=0.00..66406.12 rows=61 width=18) (actual time=46.558..614.345 rows=5 loops=1)
                    Filter: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
                    Rows Removed by Filter: 3397410
              ->  Parallel Seq Scan on user_logs user_logs_1  (cost=0.00..24045.52 rows=32 width=18) (actual time=161.086..692.922 rows=2 loops=3)
                    Filter: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
                    Rows Removed by Filter: 410079
              ->  Parallel Seq Scan on user_logs_y2021q3 user_logs_3  (cost=0.00..549.06 rows=10 width=18) (actual time=86.468..86.471 rows=0 loops=1)
                    Filter: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
                    Rows Removed by Filter: 25304
              ->  Parallel Seq Scan on user_logs_y2021q4 user_logs_4  (cost=0.00..10.96 rows=1 width=282) (actual time=0.005..0.005 rows=0 loops=1)
                    Filter: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
Planning Time: 22.197 ms
Execution Time: 1081.924 ms

Узлы с высокой стоимостью в данном запросе:

Sort Node:
Стоимость: cost=91015.52..91015.78
Описание: Этот узел сортирует результаты запроса по полю datetime.

Parallel Seq Scan on user_logs_y2021q2:
Стоимость: cost=0.00..66406.12
Описание: Параллельное сканирование таблицы user_logs_y2021q2.

Parallel Seq Scan on user_logs:
Стоимость: cost=0.00..24045.52
Описание: Параллельное сканирование основной таблицы user_logs.

Parallel Seq Scan on user_logs_y2021q3:
Стоимость: cost=0.00..549.06
Описание: Параллельное сканирование таблицы user_logs_y2021q3.

Parallel Seq Scan on user_logs_y2021q4:
Стоимость: cost=0.00..10.96
Описание: Параллельное сканирование таблицы user_logs_y2021q4.

Можно эффективно оптимизировать используя индексы.
Создадим индекс на столбец visitor_uuid и datetime для ускорения поиска по этим полям.

Добавление индекса на основной таблице user_logs:

CREATE INDEX idx_user_logs_visitor_uuid_datetime ON user_logs(visitor_uuid, datetime);

CREATE INDEX idx_user_logs_y2021q2_visitor_uuid_datetime ON user_logs_y2021q2(visitor_uuid, datetime);

CREATE INDEX idx_user_logs_y2021q3_visitor_uuid_datetime ON user_logs_y2021q3(visitor_uuid, datetime);

CREATE INDEX idx_user_logs_y2021q4_visitor_uuid_datetime ON user_logs_y2021q4(visitor_uuid, datetime);

QUERY PLAN
Sort  (cost=935.45..936.05 rows=238 width=19) (actual time=3.358..3.379 rows=10 loops=1)
  Sort Key: user_logs.datetime
  Sort Method: quicksort  Memory: 25kB
  ->  Append  (cost=5.02..926.06 rows=238 width=19) (actual time=0.158..3.261 rows=10 loops=1)
        ->  Bitmap Heap Scan on user_logs user_logs_1  (cost=5.02..295.00 rows=76 width=18) (actual time=0.153..0.589 rows=5 loops=1)
              Recheck Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
              Heap Blocks: exact=5
              ->  Bitmap Index Scan on idx_user_logs_visitor_uuid_datetime  (cost=0.00..5.00 rows=76 width=0) (actual time=0.113..0.115 rows=5 loops=1)
                    Index Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
        ->  Bitmap Heap Scan on user_logs_y2021q2 user_logs_2  (cost=5.67..559.98 rows=144 width=18) (actual time=1.133..2.198 rows=5 loops=1)
              Recheck Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
              Heap Blocks: exact=5
              ->  Bitmap Index Scan on idx_user_logs_y2021q2_visitor_uuid_datetime  (cost=0.00..5.63 rows=144 width=0) (actual time=0.734..0.735 rows=5 loops=1)
                    Index Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
        ->  Bitmap Heap Scan on user_logs_y2021q3 user_logs_3  (cost=4.54..61.72 rows=17 width=18) (actual time=0.405..0.406 rows=0 loops=1)
              Recheck Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
              ->  Bitmap Index Scan on idx_user_logs_y2021q3_visitor_uuid_datetime  (cost=0.00..4.54 rows=17 width=0) (actual time=0.401..0.402 rows=0 loops=1)
                    Index Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
        ->  Index Scan using idx_user_logs_y2021q4_visitor_uuid_datetime on user_logs_y2021q4 user_logs_4  (cost=0.14..8.16 rows=1 width=282) (actual time=0.041..0.041 rows=0 loops=1)
              Index Cond: ((visitor_uuid)::text = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'::text)
Planning Time: 8.307 ms
Execution Time: 4.722 ms

Общее время выполнения исходного запроса до оптимизации: 1081.924 ms
Общее время выполнения оптимизированного запроса: 4.722 ms

-- 8
-- ищет логи за текущий день 
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

Explain ANALYZE :
QUERY PLAN
Append  (cost=0.00..155904.52 rows=1550983 width=83) (actual time=1928.359..1930.622 rows=0 loops=1)
  ->  Seq Scan on user_logs user_logs_1  (cost=0.00..39167.25 rows=410081 width=83) (actual time=622.764..622.765 rows=0 loops=1)
        Filter: ((datetime)::date > CURRENT_DATE)
        Rows Removed by Filter: 1230243
  ->  Seq Scan on user_logs_y2021q2 user_logs_2  (cost=0.00..108164.26 rows=1132424 width=83) (actual time=1199.601..1199.602 rows=0 loops=1)
        Filter: ((datetime)::date > CURRENT_DATE)
        Rows Removed by Filter: 3397415
  ->  Seq Scan on user_logs_y2021q3 user_logs_3  (cost=0.00..805.82 rows=8435 width=83) (actual time=105.921..105.924 rows=0 loops=1)
        Filter: ((datetime)::date > CURRENT_DATE)
        Rows Removed by Filter: 25304
  ->  Seq Scan on user_logs_y2021q4 user_logs_4  (cost=0.00..12.28 rows=43 width=584) (actual time=0.030..0.038 rows=0 loops=1)
        Filter: ((datetime)::date > CURRENT_DATE)
Planning Time: 29.585 ms
Execution Time: 1935.502 ms

В плане выполнения запроса следующие узлы имеют высокую стоимость:

Seq Scan on user_logs_2 (user_logs_y2021q2):
Стоимость: 108169.30
Время выполнения: 348.546 ms
Rows Removed by Filter: 3397415
Seq Scan on user_logs_3 (user_logs_y2021q3):
Стоимость: 805.82
Время выполнения: 2.975 ms
Rows Removed by Filter: 25304
Seq Scan on user_logs_1:
Стоимость: 39167.25
Время выполнения: 214.054 ms
Rows Removed by Filter: 1230243
Seq Scan on user_logs_4 (user_logs_y2021q4):
Стоимость: 12.28
Время выполнения: 0.004 ms
Rows Removed by Filter: 0
Все эти узлы выполняют полное сканирование таблицы, что может быть медленным на больших объемах данных. 
Оптимизировать можно, добавив индекс на поле datetime, чтобы ускорить операцию фильтрации по этому полю.

-- Создаем индекс на поле datetime
CREATE INDEX idx_user_logs_datetime ON public.user_logs USING btree (datetime);

-- Исправленный  запрос с использованием индекса
EXPLAIN ANALYZE 
SELECT * 
FROM user_logs 
WHERE datetime > current_date;

QUERY PLAN
Append  (cost=0.43..37.38 rows=46 width=551) (actual time=1.150..1.154 rows=0 loops=1)
  ->  Index Scan using idx_user_logs_datetime on user_logs user_logs_1  (cost=0.43..8.45 rows=1 width=83) (actual time=0.016..0.017 rows=0 loops=1)
        Index Cond: (datetime > CURRENT_DATE)
  ->  Index Scan using user_logs_y2021q2_datetime_idx on user_logs_y2021q2 user_logs_2  (cost=0.43..8.45 rows=1 width=83) (actual time=0.525..0.526 rows=0 loops=1)
        Index Cond: (datetime > CURRENT_DATE)
  ->  Index Scan using user_logs_y2021q3_datetime_idx on user_logs_y2021q3 user_logs_3  (cost=0.29..8.30 rows=1 width=83) (actual time=0.595..0.596 rows=0 loops=1)
        Index Cond: (datetime > CURRENT_DATE)
  ->  Seq Scan on user_logs_y2021q4 user_logs_4  (cost=0.00..11.95 rows=43 width=584) (actual time=0.009..0.009 rows=0 loops=1)
        Filter: (datetime > CURRENT_DATE)
Planning Time: 1.058 ms
Execution Time: 1.241 ms

Общее время выполнения исходного запроса до оптимизации: 1935.502 ms
Общее время выполнения оптимизированного запроса: 1.241 ms

-- 9
-- определяет количество неоплаченных заказов 
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

Explain ANALYZE:
QUERY PLAN
Aggregate  (cost=60932263.35..60932263.36 rows=1 width=8) (actual time=57775.050..57775.299 rows=1 loops=1)
  ->  Nested Loop  (cost=0.30..60932263.12 rows=90 width=0) (actual time=183.605..57762.882 rows=1190 loops=1)
        ->  Seq Scan on order_statuses os  (cost=0.00..2035.34 rows=124334 width=8) (actual time=1.248..12.058 rows=124334 loops=1)
        ->  Memoize  (cost=0.30..2657.36 rows=1 width=8) (actual time=0.464..0.464 rows=0 loops=124334)
              Cache Key: os.order_id
              Cache Mode: logical
              Hits: 96650  Misses: 27684  Evictions: 0  Overflows: 0  Memory Usage: 1994kB
              ->  Index Scan using orders_order_id_idx on orders o  (cost=0.29..2657.35 rows=1 width=8) (actual time=2.072..2.072 rows=0 loops=27684)
                    Index Cond: (order_id = os.order_id)
                    Filter: ((city_id = 1) AND ((SubPlan 1) = 0))
                    Rows Removed by Filter: 1
                    SubPlan 1
                      ->  Aggregate  (cost=2657.01..2657.02 rows=1 width=8) (actual time=14.407..14.407 rows=1 loops=3958)
                            ->  Seq Scan on order_statuses os1  (cost=0.00..2657.01 rows=1 width=0) (actual time=10.066..14.403 rows=1 loops=3958)
                                  Filter: ((order_id = o.order_id) AND (status_id = 2))
                                  Rows Removed by Filter: 124333
Planning Time: 17.513 ms
Execution Time: 57785.833 ms

Nested Loop:
Стоимость: 60932263.12
Время выполнения: 57762.882 ms
Этот узел объединяет результаты двух подзапросов, что может быть медленным при большом объеме данных.
Index Scan using orders_order_id_idx:
Стоимость: 2657.35
Время выполнения: 2.072 ms
Этот узел сканирует индекс, но фильтрует результаты, что может быть неэффективным, особенно если фильтрация касается большого числа строк.
Aggregate (SubPlan 1):
Стоимость: 2657.02
Время выполнения: 14.407 ms
Этот узел выполняет агрегацию в подзапросе, что может быть медленным при большом объеме данных.
Для оптимизации предлагаю следующее:

Добавляем индексы на поля, используемые в соединении и фильтрации, чтобы ускорить выполнение запроса и меняем логику запроса

CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_city_id ON orders(city_id);
CREATE INDEX idx_order_statuses_order_id_status_id ON order_statuses(order_id, status_id);

-- Оптимизированный запрос
explain analyze
SELECT count(*)
FROM orders o
JOIN order_statuses os ON o.order_id = os.order_id
LEFT JOIN order_statuses os2 ON os2.order_id = o.order_id AND os2.status_id = 2
WHERE o.city_id = 1 AND os2.order_id IS NULL;

QUERY PLAN
Aggregate  (cost=4238.62..4238.63 rows=1 width=8) (actual time=27.252..27.254 rows=1 loops=1)
  ->  Nested Loop  (cost=2636.89..4225.43 rows=5276 width=0) (actual time=24.564..27.186 rows=1190 loops=1)
        ->  Hash Anti Join  (cost=2636.47..3040.14 rows=1175 width=8) (actual time=24.535..25.696 rows=1190 loops=1)
              Hash Cond: (o.order_id = os2.order_id)
              ->  Bitmap Heap Scan on orders o  (cost=46.96..411.44 rows=3958 width=8) (actual time=0.266..0.743 rows=3958 loops=1)
                    Recheck Cond: (city_id = 1)
                    Heap Blocks: exact=314
                    ->  Bitmap Index Scan on idx_orders_city_id  (cost=0.00..45.97 rows=3958 width=0) (actual time=0.211..0.211 rows=3958 loops=1)
                          Index Cond: (city_id = 1)
              ->  Hash  (cost=2346.18..2346.18 rows=19467 width=8) (actual time=24.236..24.236 rows=19330 loops=1)
                    Buckets: 32768  Batches: 1  Memory Usage: 1012kB
                    ->  Seq Scan on order_statuses os2  (cost=0.00..2346.18 rows=19467 width=8) (actual time=0.013..19.062 rows=19330 loops=1)
                          Filter: (status_id = 2)
                          Rows Removed by Filter: 105004
        ->  Index Only Scan using idx_order_statuses_order_id_status_id on order_statuses os  (cost=0.42..0.96 rows=5 width=8) (actual time=0.001..0.001 rows=1 loops=1190)
              Index Cond: (order_id = o.order_id)
              Heap Fetches: 0
Planning Time: 0.493 ms
Execution Time: 27.331 ms

Общее время выполнения исходного запроса до оптимизации: 57785.833 ms
Общее время выполнения оптимизированного запроса: 27.331 ms


-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего

CREATE TABLE public.order_items (
	order_id int8 NULL,
	item int8 NULL,
	count int8 NULL
);

CREATE TABLE public.dishes (
	object_id int8 NULL,
	"name" varchar(128) NULL,
	spicy int4 NULL,
	fish int4 NULL,
	meat int4 NULL,
	rest_id varchar(128) NULL
);


SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
	SELECT item
	FROM (SELECT item, SUM(count) AS total_sales
		  FROM order_items oi
		  GROUP BY 1) dishes_sales
	WHERE dishes_sales.total_sales > (
		SELECT SUM(t.total_sales)/ COUNT(*)
		FROM (SELECT item, SUM(count) AS total_sales
			FROM order_items oi
			GROUP BY
				1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;


Explain ANALYZE:
QUERY PLAN
Sort  (cost=4808.91..4810.74 rows=735 width=66) (actual time=211.650..211.806 rows=362 loops=1)
  Sort Key: (sum(oi.count)) DESC
  Sort Method: quicksort  Memory: 58kB
  InitPlan 1 (returns $0)
    ->  Aggregate  (cost=1501.65..1501.66 rows=1 width=32) (actual time=37.305..37.305 rows=1 loops=1)
          ->  HashAggregate  (cost=1480.72..1490.23 rows=761 width=40) (actual time=36.791..36.992 rows=761 loops=1)
                Group Key: oi_2.item
                Batches: 1  Memory Usage: 169kB
                ->  Seq Scan on order_items oi_2  (cost=0.00..1134.48 rows=69248 width=16) (actual time=0.015..9.198 rows=69248 loops=1)
  ->  HashAggregate  (cost=3263.06..3272.25 rows=735 width=66) (actual time=210.779..210.983 rows=362 loops=1)
        Group Key: d.name
        Batches: 1  Memory Usage: 169kB
        ->  Hash Join  (cost=1522.66..3147.65 rows=23083 width=42) (actual time=173.732..197.977 rows=35854 loops=1)
              Hash Cond: (oi.item = d.object_id)
              ->  Seq Scan on order_items oi  (cost=0.00..1134.48 rows=69248 width=16) (actual time=1.530..7.882 rows=69248 loops=1)
              ->  Hash  (cost=1519.48..1519.48 rows=254 width=50) (actual time=172.153..172.289 rows=366 loops=1)
                    Buckets: 1024  Batches: 1  Memory Usage: 39kB
                    ->  Hash Join  (cost=1497.85..1519.48 rows=254 width=50) (actual time=170.714..171.935 rows=366 loops=1)
                          Hash Cond: (d.object_id = dishes_sales.item)
                          ->  Seq Scan on dishes d  (cost=0.00..19.62 rows=762 width=42) (actual time=0.686..1.558 rows=762 loops=1)
                          ->  Hash  (cost=1494.67..1494.67 rows=254 width=8) (actual time=169.717..169.718 rows=366 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 23kB
                                ->  Subquery Scan on dishes_sales  (cost=1480.72..1494.67 rows=254 width=8) (actual time=169.140..169.411 rows=366 loops=1)
                                      ->  HashAggregate  (cost=1480.72..1492.13 rows=254 width=40) (actual time=169.138..169.358 rows=366 loops=1)
                                            Group Key: oi_1.item
                                            Filter: (sum(oi_1.count) > $0)
                                            Batches: 1  Memory Usage: 169kB
                                            Rows Removed by Filter: 395
                                            ->  Seq Scan on order_items oi_1  (cost=0.00..1134.48 rows=69248 width=16) (actual time=0.009..82.001 rows=69248 loops=1)
Planning Time: 14.123 ms
Execution Time: 217.498 ms

Узлы с высокой стоимостью в вашем запросе:
Sort:
Стоимость: 4808.91
Время выполнения: 211.806 ms
Сортировка по сумме заказов в убывающем порядке.
HashAggregate (InitPlan 1):
Стоимость: 1501.66
Время выполнения: 37.305 ms
Подзапрос для вычисления средней суммы продаж.
HashAggregate (внешний):
Стоимость: 3263.06
Время выполнения: 210.983 ms
Агрегация по названию блюда.
Hash Join (внешний):
Стоимость: 1522.66
Время выполнения: 197.977 ms
Соединение таблиц order_items и dishes.
Hash Join (внутренний):
Стоимость: 1497.85
Время выполнения: 171.935 ms
Соединение таблиц dishes и dishes_sales.
HashAggregate (Subquery Scan on dishes_sales):
Стоимость: 1480.72
Время выполнения: 169.358 ms

Агрегация для подзапроса, который фильтрует продукты с суммарными продажами выше среднего.
Для оптимизации запроса можно сделать :

Индексы:  order_items.item, dishes.object_id.
Используйте подзапрос с JOIN вместо IN, что может улучшить производительность.

CREATE INDEX idx_order_items_item ON order_items(item);
CREATE INDEX idx_dishes_object_id ON dishes(object_id);

SELECT d.name, SUM(oi.count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
JOIN (
    SELECT item, SUM(count) AS total_sales
    FROM order_items
    GROUP BY item
    HAVING SUM(count) > (SELECT SUM(total_sales) / COUNT(*) FROM (SELECT item, SUM(count) AS total_sales FROM order_items GROUP BY item) t)
) t ON oi.item = t.item
GROUP BY d.name
ORDER BY orders_quantity DESC;

QUERY PLAN
Sort  (cost=4562.67..4564.51 rows=735 width=66) (actual time=63.647..63.663 rows=362 loops=1)
  Sort Key: (sum(oi.count)) DESC
  Sort Method: quicksort  Memory: 58kB
  ->  HashAggregate  (cost=4518.49..4527.68 rows=735 width=66) (actual time=63.503..63.560 rows=362 loops=1)
        Group Key: d.name
        Batches: 1  Memory Usage: 169kB
        ->  Nested Loop  (cost=2999.81..4403.07 rows=23083 width=42) (actual time=45.937..57.620 rows=35854 loops=1)
              ->  Hash Join  (cost=2999.51..3021.15 rows=254 width=50) (actual time=45.921..46.106 rows=366 loops=1)
                    Hash Cond: (d.object_id = t.item)
                    ->  Seq Scan on dishes d  (cost=0.00..19.62 rows=762 width=42) (actual time=0.012..0.062 rows=762 loops=1)
                    ->  Hash  (cost=2996.34..2996.34 rows=254 width=8) (actual time=45.902..45.904 rows=366 loops=1)
                          Buckets: 1024  Batches: 1  Memory Usage: 23kB
                          ->  Subquery Scan on t  (cost=2982.38..2996.34 rows=254 width=8) (actual time=45.707..45.865 rows=366 loops=1)
                                ->  HashAggregate  (cost=2982.38..2993.80 rows=254 width=40) (actual time=45.706..45.837 rows=366 loops=1)
                                      Group Key: order_items.item
                                      Filter: (sum(order_items.count) > $0)
                                      Batches: 1  Memory Usage: 169kB
                                      Rows Removed by Filter: 395
                                      InitPlan 1 (returns $0)
                                        ->  Aggregate  (cost=1501.65..1501.66 rows=1 width=32) (actual time=14.692..14.693 rows=1 loops=1)
                                              ->  HashAggregate  (cost=1480.72..1490.23 rows=761 width=40) (actual time=14.539..14.637 rows=761 loops=1)
                                                    Group Key: order_items_1.item
                                                    Batches: 1  Memory Usage: 169kB
                                                    ->  Seq Scan on order_items order_items_1  (cost=0.00..1134.48 rows=69248 width=16) (actual time=0.010..3.460 rows=69248 loops=1)
                                      ->  Seq Scan on order_items  (cost=0.00..1134.48 rows=69248 width=16) (actual time=0.004..7.763 rows=69248 loops=1)
              ->  Index Scan using idx_order_items_item on order_items oi  (cost=0.29..4.53 rows=91 width=16) (actual time=0.001..0.023 rows=98 loops=366)
                    Index Cond: (item = d.object_id)
Planning Time: 0.505 ms
Execution Time: 63.778 ms

Общее время выполнения исходного запроса до оптимизации: 57785.833 ms
Общее время выполнения оптимизированного запроса: 217.498 ms