
--Шаг 1. Создайте enum cafe.restaurant_type с типом заведения coffee_shop, restaurant, bar, pizzeria:

CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

--Шаг 2. Создайте таблицу cafe.restaurants:
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE cafe.restaurants (
    restaurant_uuid uuid DEFAULT uuid_generate_v4(),
    restaurant_name varchar(50) NOT NULL,
    restaurant_location public.geometry(geometry, 4326) NULL,
    restaurant_type cafe.restaurant_type,
    menu jsonb NULL,
    PRIMARY KEY (restaurant_uuid)
);

--Шаг 3. Создайте таблицу cafe.managers:

CREATE TABLE cafe.managers (
    manager_uuid uuid DEFAULT uuid_generate_v4(),
    manager_name varchar(50) NOT NULL,
    manager_phone varchar(50) NOT NULL,
    PRIMARY KEY (manager_uuid)
);

--Шаг 4. Создайте таблицу cafe.restaurant_manager_work_dates:

CREATE TABLE cafe.restaurant_manager_work_dates (
    restaurant_uuid uuid,
    manager_uuid uuid,
    work_start_date date NOT NULL,
    work_end_date date NOT NULL,
    PRIMARY KEY (restaurant_uuid, manager_uuid),
    FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants (restaurant_uuid),
    FOREIGN KEY (manager_uuid) REFERENCES cafe.managers (manager_uuid)
);



--Шаг 5. Создайте таблицу cafe.sales со столбцами: date, restaurant_uuid, avg_check. 
--Задайте составной первичный ключ из даты и uuid ресторана

CREATE TABLE cafe.sales (
    report_date date NOT NULL,
    restaurant_uuid uuid NOT NULL,
    avg_check numeric(6, 2) NULL,
    CONSTRAINT sales_pkey PRIMARY KEY (report_date, restaurant_uuid),
    CONSTRAINT fk_sales_restaurant
        FOREIGN KEY (restaurant_uuid)
        REFERENCES cafe.restaurants (restaurant_uuid)
);



--Добавляем данные в restaurants

INSERT INTO cafe.restaurants (restaurant_name, restaurant_location, restaurant_type, menu)
select distinct
    s.cafe_name,
    ST_GeomFromText('POINT(' || s.longitude || ' ' || s.latitude || ')', 4326),
    s.type::cafe.restaurant_type,
    m.menu
FROM
    raw_data.menu m
JOIN
    raw_data.sales s ON m.cafe_name = s.cafe_name;

   
 -- Добавляем данные в managers

 INSERT INTO cafe.managers (manager_name, manager_phone)
SELECT DISTINCT
    manager,
    manager_phone
FROM
    raw_data.sales;

   
   
 --Добавляем данные в sales
   
INSERT INTO cafe.sales (report_date, restaurant_uuid, avg_check)
SELECT
    s.report_date,
    r.restaurant_uuid,
    s.avg_check
FROM
    raw_data.sales s
JOIN
    cafe.restaurants r ON s.cafe_name = r.restaurant_name;
   
 
   
--Добавляем данные в restaurant_manager_work_dates
   
   
INSERT INTO cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, work_start_date, work_end_date)
SELECT
    r.restaurant_uuid,
    m.manager_uuid,
    MIN(s.report_date) AS work_start_date ,
    MAX(s.report_date) AS work_end_date
FROM
    raw_data.sales s
JOIN
    cafe.restaurants r ON s.cafe_name = r.restaurant_name
JOIN
    cafe.managers m ON s.manager = m.manager_name
GROUP BY
    r.restaurant_uuid, m.manager_uuid;



--Задание 1 Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый высокий средний чек. 
--Создайте представление, которое покажет топ-3 заведений внутри каждого типа заведения по среднему чеку за все даты. 
--Столбец со средним чеком округлите до второго знака после запятой.

WITH AvgSalesPerRestaurant AS (
    SELECT
        r.restaurant_name,
        r.restaurant_type,
        ROUND(AVG(s.avg_check), 2) AS avg_check
    FROM
        cafe.sales s
    JOIN
        cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
    GROUP BY
        r.restaurant_name, r.restaurant_type
)

SELECT
    restaurant_name AS "Название заведения",
    restaurant_type AS "Тип заведения",
    avg_check AS "Средний чек"
FROM (
    SELECT
        restaurant_name,
        restaurant_type,
        avg_check,
        ROW_NUMBER() OVER (PARTITION BY restaurant_type ORDER BY avg_check DESC) AS rank_per_type
    FROM
        AvgSalesPerRestaurant
) ranked
WHERE
    rank_per_type <= 3;


--Задание 2Создайте материализованное представление, которое покажет, как изменяется средний чек для каждого заведения от года к году за все года за исключением 2023 года. 
--Все столбцы со средним чеком округлите до второго знака после запятой.
   
 -- Создаем материализованное представление
CREATE MATERIALIZED VIEW IF NOT EXISTS cafe.avg_check_changes AS
WITH AvgSalesPerYear AS (
    SELECT
        EXTRACT(YEAR FROM s.report_date) AS year,
        r.restaurant_name,
        r.restaurant_type,
        ROUND(AVG(s.avg_check), 2) AS avg_check
    FROM
        cafe.sales s
    JOIN
        cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
    WHERE
        EXTRACT(YEAR FROM s.report_date) != 2023 -- Исключаем 2023 год
    GROUP BY
        year, r.restaurant_name, r.restaurant_type
)

SELECT
    a.year,
    a.restaurant_name AS "Название заведения",
    a.restaurant_type AS "Тип заведения",
    a.avg_check AS "Средний чек в этом году",
    LAG(a.avg_check) OVER (PARTITION BY a.restaurant_name ORDER BY a.year) AS "Средний чек в предыдущем году",
    CASE
        WHEN LAG(a.avg_check) OVER (PARTITION BY a.restaurant_name ORDER BY a.year) IS NOT NULL
            THEN ROUND(((a.avg_check - LAG(a.avg_check) OVER (PARTITION BY a.restaurant_name ORDER BY a.year)) / LAG(a.avg_check) OVER (PARTITION BY a.restaurant_name ORDER BY a.year)) * 100, 2)
        ELSE NULL
    END AS "Изменение среднего чека в %"
FROM
    AvgSalesPerYear a;

-- Создаем индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_avg_check_changes_cafe_name ON cafe.avg_check_changes (cafe_name);
CREATE INDEX IF NOT EXISTS idx_avg_check_changes_year ON cafe.avg_check_changes (year);

SELECT * FROM cafe.avg_check_changes;


--Задание 3 Найдите топ-3 заведения, где чаще всего менялся менеджер за весь период.

SELECT
    r.restaurant_name AS "Название заведения",
    COUNT(DISTINCT rm.manager_uuid) AS "Сколько раз менялся менеджер"
FROM
    cafe.restaurants r
JOIN
    cafe.restaurant_manager_work_dates rm ON r.restaurant_uuid = rm.restaurant_uuid
GROUP BY
    r.restaurant_name
ORDER BY
    "Сколько раз менялся менеджер" DESC
LIMIT 3;


--Задание 4 Найдите пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведите все.

WITH PizzaCounts AS (
    SELECT
        r.restaurant_name AS "Название заведения",
        COUNT(DISTINCT menu_item.value) AS "Количество пицц в меню"
    FROM
        cafe.restaurants r
    CROSS JOIN
        LATERAL jsonb_each_text(r.menu) AS menu_item
    WHERE
        r.restaurant_type = 'pizzeria'
    GROUP BY
        r.restaurant_name
    ORDER BY
        "Количество пицц в меню" DESC
)

SELECT
    "Название заведения",
    "Количество пицц в меню"
FROM
    PizzaCounts
WHERE
    "Количество пицц в меню" = (SELECT MAX("Количество пицц в меню") FROM PizzaCounts);


--Задание 5 Найдите самую дорогую пиццу для каждой пиццерии.
  
   WITH menu_cte AS (
    SELECT
        restaurant_name AS "Название заведения",
        'Пицца' AS "Тип блюда",
        menu_item.key AS "Название пиццы",
        CAST(menu_item.value AS NUMERIC) AS "Цена"
    FROM
        cafe.restaurants
    CROSS JOIN
        LATERAL jsonb_each(menu->'Пицца') AS menu_item
    WHERE
        restaurant_type = 'pizzeria'
),
menu_with_rank AS (
    SELECT
        "Название заведения",
        "Тип блюда",
        "Название пиццы",
        "Цена",
        ROW_NUMBER() OVER (PARTITION BY "Название заведения" ORDER BY "Цена" DESC) AS price_rank
    FROM
        menu_cte
)

SELECT
    "Название заведения",
    "Тип блюда",
    "Название пиццы",
    "Цена"
FROM
    menu_with_rank
WHERE
    price_rank = 1
ORDER BY
    "Название заведения";

--Задание 6 Найдите два самых близких друг к другу заведения одного типа
   
 WITH dist AS (
    SELECT
        r1.restaurant_name AS rest1,
        r2.restaurant_name AS rest2,
        r1.restaurant_type AS type,
        ST_Distance(r1.restaurant_location::geography, r2.restaurant_location::geography) AS distance
    FROM
        cafe.restaurants r1
    JOIN
        cafe.restaurants r2 ON r1.restaurant_type = r2.restaurant_type AND r1.restaurant_name < r2.restaurant_name
)

SELECT
    rest1 AS "Название Заведения 1",
    rest2 AS "Название Заведения 2",
    type AS "Тип заведения",
    distance AS "Расстояние"
FROM
    dist
ORDER BY
    distance
LIMIT 2;

--Задание 7 Найдите район с самым большим количеством заведений и район с самым маленьким количеством заведений. 
--Первой строчкой выведите район с самым большим количеством заведений, второй — с самым маленьким

WITH RestaurantCounts AS (
    SELECT
        d.district_name,
        COUNT(r.restaurant_uuid) AS num_restaurants,
        RANK() OVER (ORDER BY COUNT(r.restaurant_uuid) DESC) AS rank_desc,
        RANK() OVER (ORDER BY COUNT(r.restaurant_uuid) ASC) AS rank_asc
    FROM
        cafe.districts d
    LEFT JOIN
        cafe.restaurants r ON ST_Within(r.restaurant_location::geometry, d.district_geom::geometry)
    GROUP BY
        d.district_name
)

SELECT
    district_name AS "Название района",
    num_restaurants AS "Количество заведений"
FROM
    RestaurantCounts
WHERE
    rank_desc = 1

UNION ALL

SELECT
    district_name AS "Название района",
    num_restaurants AS "Количество заведений"
FROM
    RestaurantCounts
WHERE
    rank_asc = 1;


