CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sales (
    id INTEGER PRIMARY KEY,
    auto VARCHAR(255),
    gasoline_consumption DECIMAL(5, 2) DEFAULT NULL,
    price DECIMAL(10, 2),
    date DATE,
    person_name VARCHAR(255),pr
    phone VARCHAR(30),
    discount DECIMAL(5, 2),
    brand_origin VARCHAR(50)
);



COPY raw_data.sales(id, auto, gasoline_consumption, price, date, person, phone, discount, brand_origin)
FROM '/Users/dasha/Desktop/работа/Project/cars_processed.csv' WITH CSV HEADER DELIMITER ',' NULL '';

-- Создание схемы
CREATE SCHEMA car_shop;

-- Таблица Color

CREATE TABLE car_shop.Color (
    color_id SERIAL PRIMARY KEY,
    color_name VARCHAR(20) NOT NULL -- Цвет автомобиля, не может быть пустым
);


-- Таблица Country
CREATE TABLE car_shop.Country (
    country_id SERIAL PRIMARY KEY, -- Идентификатор страны, увеличивается автоматически
    country_name VARCHAR(50) --NOT NULL -- Название страны, не может быть пустым и не может быть больше 50
);


--Таблица Brand
CREATE TABLE car_shop.Brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(50) NOT NULL,
    country_id INTEGER,
    CONSTRAINT fk_brand_country
        FOREIGN KEY (country_id)
        REFERENCES car_shop.Country(country_id)
);


-- Таблица Model
CREATE TABLE car_shop.Model (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(50) NOT NULL, -- Модель автомобиля, не может быть пустой
    brand_id INTEGER,
    gasoline_consumption DECIMAL(5, 2),-- Расход топлива, числовое значение с двумя знаками после запятой
    CONSTRAINT fk_model_brand
    FOREIGN KEY (brand_id)
    REFERENCES car_shop.Brand(brand_id)
);

-- Таблица Car
CREATE TABLE car_shop.Car (
    car_id SERIAL PRIMARY KEY, -- Идентификатор автомобиля, увеличивается автоматически
    model_id INTEGER,
    color_id INTEGER,
    country_id INTEGER, -- Связь с таблицей Country через country_id 
    CONSTRAINT fk_car_country
    FOREIGN KEY (country_id)
    REFERENCES car_shop.Country(country_id), -- Ограничение внешнего ключа для country_id
    CONSTRAINT fk_car_model
    FOREIGN KEY (model_id)
    REFERENCES car_shop.Model(model_id), -- Ограничение внешнего ключа для model_id
    CONSTRAINT fk_car_color
    FOREIGN KEY (color_id)
    REFERENCES car_shop.Color(color_id) -- Ограничение внешнего ключа для color_id
);

--Таблица Customer
CREATE TABLE car_shop.Customer (
    customer_id SERIAL PRIMARY KEY,
    person_name VARCHAR(100) NOT NULL,-- Имя покупателя, не может быть пустым
    phone VARCHAR(30) NOT NULL-- Номер телефона покупателя, не может быть пустым и 20 или 10 для номера будет мало
);

-- Таблица Sales

CREATE TABLE car_shop.Sale (
    sale_id SERIAL PRIMARY KEY,-- Идентификатор продажи, увеличивается автоматически
    car_id INTEGER REFERENCES car_shop.Car(car_id),-- Связь с таблицей Car по car_id
    customer_id INTEGER REFERENCES car_shop.Customer(customer_id),
    sale_date DATE NOT NULL,-- Дата продажи, не может быть пустой
    price DECIMAL(15, 2) NOT NULL,-- Цена автомобиля, не может быть пустой
    discount INTEGER,-- Скидка (может быть пустой)
    CONSTRAINT valid_discount CHECK (discount >= 0 AND discount <= 100)-- Ограничение на диапазон скидки (0-100%)
);



-- Заполнение таблицы Country
INSERT INTO car_shop.Country (country_name)
SELECT DISTINCT
    brand_origin 
FROM
    raw_data.sales
WHERE brand_origin IS NOT NULL;
    


-- Заполнение таблицы Brand
INSERT INTO car_shop.Brand (brand_name,country_id)
SELECT DISTINCT
    split_part(auto, ' ', 1) AS brand, c.country_id
FROM
    raw_data.sales r
JOIN
    car_shop.Country c ON c.country_name = r.brand_origin
 ;

-- Заполнение таблицы Model

INSERT INTO car_shop.Model (model_name,brand_id,gasoline_consumption)
SELECT DISTINCT
    TRIM(BOTH ',' FROM
        CASE 
            WHEN split_part(r.auto, ' ', 2) = 'Model' THEN 
                CONCAT(split_part(r.auto, ' ', 2), ' ', split_part(r.auto, ' ', 3))
            ELSE replace(split_part(r.auto, ' ', 2), ',', '')
        END) AS model,
    b.brand_id,
    r.gasoline_consumption
FROM
    raw_data.sales r
left JOIN
    car_shop.Brand b ON b.brand_name = split_part(r.auto, ' ', 1);


 -- Заполнение таблицы Color

INSERT INTO car_shop.Color (color_name)
   SELECT DISTINCT
    COALESCE(split_part(auto, ' ', 4), replace(split_part(auto, ' ', 3), ',', '')) AS color_name
FROM
    raw_data.sales r
where COALESCE(split_part(auto, ' ', 4), replace(split_part(auto, ' ', 3), ',', '')) <> '';


-- Заполнение таблицы Car
  
INSERT INTO car_shop.Car (model_id, color_id,country_id)
SELECT
    m.model_id,
    cl.color_id,
    cn.country_id
FROM
    raw_data.sales r
JOIN
    car_shop.Model m ON m.model_name = TRIM(BOTH ',' FROM
        CASE 
            WHEN split_part(r.auto, ' ', 2) = 'Model' THEN 
                CONCAT(split_part(r.auto, ' ', 2), ' ', split_part(r.auto, ' ', 3))
            ELSE replace(split_part(r.auto, ' ', 2), ',', '')
        END
    )
JOIN
    car_shop.Color cl ON cl.color_name IN (
        COALESCE(split_part(r.auto, ' ', 4), ''),
        replace(split_part(r.auto, ' ', 3), ',', '')
    )
JOIN
    car_shop.Country cn ON cn.country_name = r.brand_origin;
   
      

---Заполнение таблицы customer
   
 INSERT INTO car_shop.customer  (person_name,phone)
select distinct person,phone
from raw_data.sales r;



-- Заполнение таблицы Sale
INSERT INTO car_shop.Sale (car_id, sale_date, discount,price,customer_id)
SELECT
    c.car_id,
    r.date AS sale_date,
    r.discount,
    r.price,
    cs.customer_id
FROM
    raw_data.sales r
JOIN
    car_shop.Car c ON r.id = c.car_id
JOIN
    car_shop.Customer cs ON cs.phone = r.phone 
;


-- Напишите запрос, который выведет процент моделей машин, у которых нет параметра gasoline_consumption
SELECT
    (COUNT(*) FILTER (WHERE gasoline_consumption IS NULL) * 100.0 / COUNT(*)) AS nulls_percentage_gasoline_consumption
FROM
    car_shop.model;

--Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки. 

SELECT
    brand_name,
    EXTRACT(YEAR FROM s.sale_date) AS year,
    ROUND(AVG(s.price * (1 - (s.discount/100))), 2) AS price_avg
FROM
    car_shop.Car c
JOIN
    car_shop.Sale s ON c.car_id = s.car_id
JOIN
    car_shop.model m  ON c.model_id = m.model_id
JOIN
    car_shop.brand b ON m.brand_id = b.brand_id
WHERE
    s.price IS NOT NULL
GROUP BY
    b.brand_name, EXTRACT(YEAR FROM s.sale_date)
ORDER BY
    b.brand_name, year;

 
 
--Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки. 
--Результат отсортируйте по месяцам в восходящем порядке.Среднюю цену округлите до второго знака после запятой.

SELECT
    EXTRACT(MONTH FROM s.sale_date) AS month,
    ROUND(AVG(s.price * (1 - (s.discount/100))), 2) AS price_avg
FROM
    car_shop.sale s 
WHERE
    EXTRACT(YEAR FROM s.sale_date) = 2022
GROUP BY
    month
ORDER BY
    month;
   
   
   
 --Используя функцию STRING_AGG, напишите запрос, который выведет список купленных машин у каждого пользователя через запятую. 
 --Пользователь может купить две одинаковые машины — это нормально. Название машины покажите полное, с названием бренда — например: Tesla Model 3. 
 --Отсортируйте по имени пользователя в восходящем порядке. Сортировка внутри самой строки с машинами не нужна.
   
 SELECT
    cs.person_name  AS person,
    STRING_AGG(b.brand_name || ' ' || m.model_name, ', ') AS cars
FROM
    car_shop.Sale s
JOIN
    car_shop.Car c ON s.car_id = c.car_id
JOIN
    car_shop.Customer cs ON s.customer_id = cs.customer_id
JOIN
    car_shop.Model m ON c.model_id = m.model_id
JOIN
    car_shop.Brand b ON m.brand_id = b.brand_id
GROUP BY
    cs.person_name
ORDER BY
    person;


--Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране без учёта скидки. 
--Цена в колонке price дана с учётом скидки.
   
SELECT
    cn.country_name AS brand_origin,
    MAX(s.price * (1 - (s.discount/100))) AS price_max,
    MIN(s.price * (1 - (s.discount/100))) AS price_min
FROM
    car_shop.Sale s
JOIN
    car_shop.Car c ON s.car_id = c.car_id
JOIN
    car_shop.Country cn ON c.country_id = cn.country_id
GROUP BY
    cn.country_name ;

--Напишите запрос, который покажет количество всех пользователей из США. Это пользователи, у которых номер телефона начинается на +1.
   
SELECT
    COUNT(DISTINCT cs.person_name) AS persons_from_usa_count
FROM
    car_shop.Customer cs
WHERE
    cs.phone LIKE '+1%';