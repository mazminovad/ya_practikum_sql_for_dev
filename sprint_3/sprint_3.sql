
--Задание 1 Напишите хранимую процедуру update_employees_rate, которая обновляет почасовую ставку сотрудников на определённый процент. 
--При понижении ставка не может быть ниже минимальной — 500 рублей в час. 
--Если по расчётам выходит меньше, устанавливают минимальную ставку.

CREATE OR REPLACE PROCEDURE update_employees_rate(rate_changes_json json)
LANGUAGE plpgsql
AS $$
DECLARE
    rate_change_record json;
    employee_id_val uuid;
    rate_change_val int;
    new_rate_val int;
BEGIN
    -- Итерируемся по каждой записи в переданном JSON
    FOR rate_change_record IN SELECT * FROM json_array_elements(rate_changes_json)
    LOOP
        -- Извлекаем значения из JSON
        employee_id_val := rate_change_record->>'employee_id';
        rate_change_val := (rate_change_record->>'rate_change')::int;

        -- Проверяем, существует ли сотрудник с таким идентификатором
        IF NOT EXISTS (SELECT 1 FROM public.employees WHERE id = employee_id_val) THEN
            RAISE EXCEPTION 'Employee with id % not found', employee_id_val;
        END IF;

        -- Получаем текущую ставку сотрудника
        SELECT rate INTO new_rate_val FROM public.employees WHERE id = employee_id_val;

        -- Обновляем ставку на заданный процент
        new_rate_val := new_rate_val + (new_rate_val * rate_change_val / 100);

        -- Проверяем, не опустилась ли ставка ниже минимальной
        IF new_rate_val < 500 THEN
            new_rate_val := 500;
        END IF;

        -- Обновляем ставку сотрудника
        UPDATE public.employees SET rate = new_rate_val WHERE id = employee_id_val;
    END LOOP;
END;
$$;


CALL update_employees_rate(
    '[
        {"employee_id": "e9315f02-bc75-42bf-87b9-653faddbccd9", "rate_change": 10}, 
        {"employee_id": "a43e64ee-2603-40e1-9ce9-8c4b33807eed", "rate_change": -5}
    ]'::json
);


select * from employees

--Задание 2
--Напишите хранимую процедуру indexing_salary, которая повышает зарплаты всех сотрудников на определённый процент. 
--Процедура принимает один целочисленный параметр — процент индексации p. Сотрудникам, которые получают зарплату по ставке ниже средней относительно всех сотрудников до индексации, начисляют дополнительные 2% (p + 2). 
--Ставка остальных сотрудников увеличивается на p%.
--Зарплата хранится в БД в типе данных integer, поэтому если в результате повышения зарплаты образуется дробное число, его нужно округлить до целого.


CREATE OR REPLACE FUNCTION indexing_salary(p_percent INT) RETURNS VOID AS
$$
DECLARE
    v_avg_salary INT;
BEGIN
    -- Рассчитываем среднюю зарплату всех сотрудников
    SELECT ROUND(AVG(rate)) INTO v_avg_salary
    FROM public.employees;

    -- Повышаем зарплаты сотрудников на указанный процент
    UPDATE public.employees
    SET rate = 
        CASE
            WHEN rate < v_avg_salary THEN ROUND(rate * (1 + p_percent / 100 + 0.02))
            ELSE ROUND(rate * (1 + p_percent / 100))
        END;

    --COMMIT;
END;
$$ LANGUAGE plpgsql;

--Задание 3
--Завершая проект, нужно сделать два действия в системе учёта:
--Изменить значение поля is_active в записи проекта на false — чтобы рабочее время по этому проекту больше не учитывалось.
--Посчитать бонус, если он есть — то есть распределить неизрасходованное время между всеми членами команды проекта. Неизрасходованное время — это разница между временем, 
--которое выделили на проект (estimated_time), и фактически потраченным. 
--Если поле estimated_time не задано, бонусные часы не распределятся.

CREATE OR REPLACE PROCEDURE close_project(project_id UUID) AS
$$
DECLARE
    v_project_closed BOOLEAN;
    v_estimated_time INT;
    v_actual_time INT;
    v_savings INT;
    v_bonus_per_member INT;
BEGIN
    -- Проверяем, был ли проект уже закрыт
    SELECT is_active INTO v_project_closed
    FROM public.projects
    WHERE id = project_id;

    IF NOT v_project_closed THEN
        RAISE EXCEPTION 'Проект уже закрыт';
    END IF;

    -- Получаем информацию о проекте
    SELECT estimated_time INTO v_estimated_time
    FROM public.projects
    WHERE id = project_id;

    -- Получаем фактически потраченное время над проектом
    SELECT SUM(work_hours) INTO v_actual_time
    FROM public.logs
    WHERE project_id = project_id;

    -- Рассчитываем сэкономленное время
    v_savings := GREATEST(v_estimated_time - v_actual_time, 0);

    -- Если есть бонусные часы
    IF v_savings > 0 THEN
        -- Рассчитываем бонус на каждого участника проекта
        SELECT FLOOR(LEAST(v_savings * 0.75 / NULLIF((SELECT COUNT(*) FROM public.logs WHERE project_id = project_id), 0), 16))
        INTO v_bonus_per_member;

        -- Распределяем бонусные часы между участниками проекта
        UPDATE public.employees
        SET rate = rate + v_bonus_per_member
        WHERE id IN (SELECT employee_id FROM public.logs WHERE project_id = project_id);

        -- Записываем в логи бонусные часы с текущей датой
        INSERT INTO public.logs (employee_id, project_id, work_date, work_hours, is_paid)
        SELECT employee_id, project_id, CURRENT_DATE, v_bonus_per_member, true
        FROM public.logs
        WHERE project_id = project_id;
    END IF;

    -- Закрываем проект
    UPDATE public.projects
    SET is_active = false
    WHERE id = project_id;
END;
$$ LANGUAGE plpgsql;



--Задание 4
--Напишите процедуру log_work для внесения отработанных сотрудниками часов. Процедура добавляет новые записи о работе сотрудников над проектами.
--Процедура принимает id сотрудника, id проекта, дату и отработанные часы и вносит данные в таблицу logs. 
--Если проект завершён, добавить логи нельзя — процедура должна вернуть ошибку Project closed. Количество часов не может быть меньше 1 или больше 24. 
--Если количество часов выходит за эти пределы, необходимо вывести предупреждение о недопустимых данных и остановить выполнение процедуры.
--При логировании более 16 часов в день запись помечается флагом required_review — Dream Big заботится о здоровье сотрудников. 
--Также флагом required_review помечаются записи будущим числом или числом больше чем на неделю назад от текущего дня.

CREATE OR REPLACE PROCEDURE log_work(
    p_employee_id UUID,
    p_project_id UUID,
    p_work_date DATE,
    p_work_hours INT
) AS
$$
DECLARE
    v_project_active BOOLEAN;
    v_hours_warning TEXT;
BEGIN
    -- Проверка, завершен ли проект
    SELECT is_active INTO v_project_active
    FROM public.projects
    WHERE id = p_project_id;

    IF NOT v_project_active THEN
        RAISE EXCEPTION 'Project closed';
    END IF;

    -- Проверка корректности количества отработанных часов
    IF p_work_hours < 1 OR p_work_hours > 24 THEN
        RAISE EXCEPTION 'Invalid number of worked hours';
    END IF;

    -- Проверка на флаг required_review
    IF p_work_hours > 16 OR p_work_date > CURRENT_DATE OR p_work_date < CURRENT_DATE - INTERVAL '7 days' THEN
        v_hours_warning := 'true';
    ELSE
        v_hours_warning := 'false';
    END IF;

    -- Внесение записи в таблицу logs
    INSERT INTO public.logs (employee_id, project_id, work_date, work_hours, required_review, is_paid)
    VALUES (p_employee_id, p_project_id, p_work_date, p_work_hours, v_hours_warning::BOOLEAN, false);

END;
$$ LANGUAGE plpgsql;



--Задание 5 
--Чтобы бухгалтерия корректно начисляла зарплату, нужно хранить историю изменения почасовой ставки сотрудников. 
--Создайте отдельную таблицу employee_rate_history с такими столбцами:
--id — id записи,
--employee_id — id сотрудника,
--rate — почасовая ставка сотрудника,
--from_date — дата назначения новой ставки.
--Внесите в таблицу текущие данные всех сотрудников. В качестве from_date используйте дату основания компании: '2020-12-26'.
--Напишите триггерную функцию save_employee_rate_history и триггер change_employee_rate. При добавлении сотрудника в таблицу employees и изменении ставки сотрудника триггер автоматически вносит запись в таблицу employee_rate_history из трёх полей: id сотрудника, его ставки и текущей даты.


-- Создание таблицы employee_rate_history
CREATE TABLE IF NOT EXISTS public.employee_rate_history (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    employee_id UUID,
    rate INT,
    from_date DATE
);

-- Создание триггерной функции save_employee_rate_history
CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO public.employee_rate_history (employee_id, rate, from_date)
    VALUES (NEW.id, NEW.rate, CURRENT_DATE);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создание триггера change_employee_rate для таблицы employees
CREATE TRIGGER change_employee_rate
AFTER INSERT OR UPDATE OF rate ON public.employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();


--Задание 6
--После завершения каждого проекта Dream Big проводит корпоративную вечеринку, чтобы отпраздновать очередной успех и поощрить сотрудников. 
--Тех, кто посвятил проекту больше всего часов, награждают премией «Айтиголик» — они получают почётные грамоты и ценные подарки от заказчика.
--Чтобы вычислить айтиголиков проекта, напишите функцию best_project_workers.
--Функция принимает id проекта и возвращает таблицу с именами трёх сотрудников, которые залогировали максимальное количество часов в этом проекте. Результирующая таблица состоит из двух полей: имени сотрудника и количества часов, отработанных на проекте.

CREATE OR REPLACE FUNCTION best_project_workers(project_id UUID)
RETURNS TABLE (
    employee_name TEXT,
    work_hours INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.name AS employee_name,
        SUM(l.work_hours) AS work_hours
    FROM
        public.logs l
    JOIN
        public.employees e ON l.employee_id = e.id
    WHERE
        l.project_id = project_id
    GROUP BY
        e.name
    ORDER BY
        work_hours DESC
    LIMIT 3;

    RETURN;
END;
$$ LANGUAGE plpgsql;











