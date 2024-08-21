/*
                                          //////////////////////////////
                                         //          SOURCE          //
                                        //////////////////////////////
*/
--=================== ПОДЛЮЧЕНИЕ К ИСТОЧНИКУ И КОПИРОВАНИЕ ДАННЫХ ==================--
CREATE EXTENSION postgres_fdw;

CREATE SERVER film_pg FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    host '127.0.0.1',
    dbname 'dvdrental',
    port '5432'
    );

CREATE USER MAPPING FOR postgres SERVER film_pg OPTIONS (
    USER 'postgres',
    PASSWORD 'root'
    );

DROP SCHEMA IF EXISTS film_src;
CREATE SCHEMA film_src AUTHORIZATION postgres;

-- Создание типов данных, как в источнике
DROP TYPE IF EXISTS public.mpaa_rating;
CREATE TYPE public.mpaa_rating AS ENUM (
    'G',
    'PG',
    'PG-13',
    'R',
    'NC-17');

CREATE DOMAIN public.year AS INTEGER CHECK (VALUE >= 1901 AND VALUE <= 2155);

-- Импорт данных из базы источника в свою сырую схему
IMPORT FOREIGN SCHEMA public FROM SERVER film_pg INTO film_src;

-- Создание основных слоев (схем)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- Внесение в ручную изменений из источника в foreign tables
ALTER TABLE public.payment
    ADD COLUMN deleted TIMESTAMP;
ALTER TABLE public.payment
    ADD COLUMN last_update TIMESTAMP;

ALTER TABLE film_src.payment
    ADD COLUMN deleted TIMESTAMP OPTIONS (COLUMN_NAME 'deleted');
ALTER TABLE film_src.payment
    ADD COLUMN last_update TIMESTAMP OPTIONS (COLUMN_NAME 'last_update');

-- Процедура, которая наполняет таблицу rental новыми записями.
-- Принимает параметры:
-- 		nm integer - число строк, которое нужно добавить
-- 		dt date default null - дата rental_date, за которую нужно добавить новые записи. Если дата не задана, то находим максимальную существующую дату rental_date в таблице rental и прибавляем к ней один день.
-- Компакт диски для сдачи выбираются случайным образом.

CREATE PROCEDURE public.fill_rental(nm INTEGER, dt DATE DEFAULT NULL)
AS
$$
INSERT INTO
    rental (rental_date, inventory_id, customer_id, return_date, staff_id)
SELECT
    subquery.rental_date,
    subquery.inventory_id,
    subquery.customer_id,

    subquery.rental_date +
    (SELECT
         f.rental_duration
     FROM
         film f
             JOIN inventory i USING (film_id)
     WHERE
         i.inventory_id = subquery.inventory_id) AS return_date,

    subquery.staff_id
FROM
    (SELECT
         COALESCE(dt, (SELECT MAX(rental_date)::DATE + 1 FROM rental)) AS rental_date,

         (SELECT
              FLOOR(rand.rand * COUNT(*)) + 1
          FROM
              inventory)                                               AS inventory_id,

         (SELECT
              FLOOR(rand.rand * COUNT(*)) + 1
          FROM
              customer)                                                AS customer_id,

         (SELECT
              FLOOR(rand.rand * COUNT(*)) + 1
          FROM
              staff)                                                   AS staff_id
     FROM
         (SELECT
              RANDOM() rand
          FROM
              GENERATE_SERIES(1, nm)) AS rand) AS subquery;

$$ LANGUAGE SQL;
--==================================================================================--
/*
                                          /////////////////////////////
                                         //         STAGING         //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц Staging-слоя
CREATE OR REPLACE PROCEDURE staging.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS staging.last_update_data;
    DROP TABLE IF EXISTS staging.film;
    DROP TABLE IF EXISTS staging.inventory;
    DROP TABLE IF EXISTS staging.rental;
    DROP TABLE IF EXISTS staging.payment;
    DROP TABLE IF EXISTS staging.staff;
    DROP TABLE IF EXISTS staging.address;
    DROP TABLE IF EXISTS staging.city;
    DROP TABLE IF EXISTS staging.store;

    -- Техническая таблица для инкрементальной загрузки данных из источника
    CREATE TABLE staging.last_update_data
    (
        table_name VARCHAR(100) NOT NULL,
        update_dt  TIMESTAMP    NOT NULL
    );

    -- Фильмы
    CREATE TABLE staging.film
    (
        film_id          INT           NOT NULL,
        title            VARCHAR(255)  NOT NULL,
        description      TEXT          NULL,
        release_year     int2          NULL,
        language_id      int2          NOT NULL,
        rental_duration  int2          NOT NULL,
        rental_rate      NUMERIC(4, 2) NOT NULL,
        length           int2          NULL,
        replacement_cost NUMERIC(5, 2) NOT NULL,
        rating           VARCHAR(10)   NULL,
        last_update      TIMESTAMP     NOT NULL,
        special_features _text         NULL,
        fulltext         tsvector      NOT NULL
    );

    -- Инвентарь
    CREATE TABLE staging.inventory
    (
        inventory_id int4      NOT NULL,
        film_id      int2      NOT NULL,
        store_id     int2      NOT NULL,
        last_update  TIMESTAMP NOT NULL,
        deleted      TIMESTAMP NULL
    );

    -- Персонал (Сотрудники)
    CREATE TABLE staging.staff
    (
        staff_id    int4        NOT NULL,
        first_name  VARCHAR(45) NOT NULL,
        last_name   VARCHAR(45) NOT NULL,
        store_id    int2        NOT NULL,
        last_update TIMESTAMP   NOT NULL,
        deleted     TIMESTAMP   NULL
    );

    -- Аренда (Прокат)
    CREATE TABLE staging.rental
    (
        rental_id    int4      NOT NULL,
        rental_date  TIMESTAMP NOT NULL,
        inventory_id int4      NOT NULL,
        customer_id  int2      NOT NULL,
        return_date  TIMESTAMP,
        staff_id     int2      NOT NULL,
        last_update  TIMESTAMP NOT NULL,
        deleted      TIMESTAMP NULL
    );

    -- Платежи
    CREATE TABLE staging.payment
    (
        payment_id   int4          NOT NULL,
        customer_id  int2          NOT NULL,
        staff_id     int2          NOT NULL,
        rental_id    int4          NOT NULL,
        inventory_id int4          NOT NULL,
        amount       NUMERIC(5, 2) NOT NULL,
        payment_date TIMESTAMP     NOT NULL,
        last_update  TIMESTAMP     NOT NULL,
        deleted      TIMESTAMP     NULL
    );

    -- Адреса
    CREATE TABLE staging.address
    (
        address_id int4        NOT NULL,
        address    VARCHAR(50) NOT NULL,
        district   VARCHAR(20) NOT NULL,
        city_id    int2        NOT NULL
    );

    -- Города
    CREATE TABLE staging.city
    (
        city_id int4        NOT NULL,
        city    VARCHAR(50) NOT NULL
    );

    -- Магазины
    CREATE TABLE staging.store
    (
        store_id   int4 NOT NULL,
        address_id int2 NOT NULL
    );
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--======================== СОЗДАНИЕ ДОП. ПРОЦЕДУР И ФУНКЦИЙ ========================--
-- Функция, возвращающая время последней загрузки в таблицы
CREATE OR REPLACE FUNCTION staging.get_last_update_table(table_name VARCHAR) RETURNS TIMESTAMP
AS
$$
BEGIN
    RETURN COALESCE(
            (SELECT
                 MAX(update_dt)
             FROM
                 staging.last_update_data lu
             WHERE
                 lu.table_name = get_last_update_table.table_name),
            '1900-01-01'::DATE);
END;
$$ LANGUAGE plpgsql;

-- Процедура вставки данных в last_update_data, в которой содержится информация по времени загрузки данных в конкретную таблицу
CREATE OR REPLACE PROCEDURE staging.set_table_load_time(table_name VARCHAR, current_update_dt TIMESTAMP DEFAULT NOW())
AS
$$
BEGIN
    INSERT INTO
        staging.last_update_data
    (table_name,
     update_dt)
    VALUES
        (table_name,
         current_update_dt);
END;
$$
    LANGUAGE plpgsql;

--=============================== ЗАПОЛНЕНИЕ ДАННЫМИ ===============================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE staging.load_film(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.film;
    INSERT INTO
        staging.film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length,
                      replacement_cost, rating, last_update, special_features, fulltext)
    SELECT
        film_id,
        title,
        description,
        release_year,
        language_id,
        rental_duration,
        rental_rate,
        length,
        replacement_cost,
        rating,
        last_update,
        special_features,
        fulltext
    FROM
        film_src.film;

    CALL staging.set_table_load_time('staging.film', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы inventory
CREATE OR REPLACE PROCEDURE staging.load_inventory(current_update_dt TIMESTAMP)
AS
$$
DECLARE
    var_last_update_dt TIMESTAMP;
BEGIN
    var_last_update_dt = staging.get_last_update_table('staging.inventory');

    TRUNCATE TABLE staging.inventory;
    INSERT INTO
        staging.inventory (inventory_id, film_id, store_id, last_update, deleted)
    SELECT
        inventory_id,
        film_id,
        store_id,
        last_update,
        deleted
    FROM
        film_src.inventory
    WHERE
         last_update >= var_last_update_dt
      OR deleted >= var_last_update_dt;

    CALL staging.set_table_load_time('staging.inventory', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы staff
CREATE OR REPLACE PROCEDURE staging.load_staff(current_update_dt TIMESTAMP)
AS
$$
DECLARE
    var_last_update_dt TIMESTAMP;
BEGIN
    var_last_update_dt = staging.get_last_update_table('staging.staff');

    TRUNCATE TABLE staging.staff;
    INSERT INTO
        staging.staff (staff_id, first_name, last_name, store_id, last_update, deleted)
    SELECT
        staff_id,
        first_name,
        last_name,
        store_id,
        last_update,
        deleted
    FROM
        film_src.staff
    WHERE
         last_update >= var_last_update_dt
      OR deleted >= var_last_update_dt;

    CALL staging.set_table_load_time('staging.staff', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE staging.load_rental(current_update_dt TIMESTAMP)
AS
$$
DECLARE
    var_last_update_dt TIMESTAMP;
BEGIN
    var_last_update_dt = staging.get_last_update_table('staging.rental');

    TRUNCATE TABLE staging.rental;
    INSERT INTO
        staging.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update, deleted)
    SELECT
        rental_id,
        rental_date,
        inventory_id,
        customer_id,
        return_date,
        staff_id,
        last_update,
        deleted
    FROM
        film_src.rental
    WHERE
         last_update >= var_last_update_dt
      OR deleted >= var_last_update_dt;

    CALL staging.set_table_load_time('staging.rental', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы payment
CREATE OR REPLACE PROCEDURE staging.load_payment(current_update_dt TIMESTAMP)
AS
$$
DECLARE
    var_last_update_dt TIMESTAMP;
BEGIN
    var_last_update_dt = staging.get_last_update_table('staging.payment');

    TRUNCATE TABLE staging.payment;
    INSERT INTO
        staging.payment (payment_id, customer_id, staff_id, rental_id, inventory_id, amount, payment_date, last_update,
                         deleted)
    SELECT
        p.payment_id,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        r.inventory_id,
        p.amount,
        p.payment_date,
        p.last_update,
        p.deleted
    FROM
        film_src.payment p
            JOIN film_src.rental r USING (rental_id)
    WHERE
         p.deleted >= var_last_update_dt
      OR p.last_update >= var_last_update_dt
      OR r.last_update >= var_last_update_dt;

    CALL staging.set_table_load_time('staging.payment', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы address
CREATE OR REPLACE PROCEDURE staging.load_address()
AS
$$
BEGIN
    TRUNCATE TABLE staging.address;
    INSERT INTO staging.address (address_id, address, district, city_id)
    SELECT
        address_id,
        address,
        district,
        city_id
    FROM
        film_src.address;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы city
CREATE OR REPLACE PROCEDURE staging.load_city()
AS
$$
BEGIN
    TRUNCATE TABLE staging.city;
    INSERT INTO staging.city (city_id, city)
    SELECT
        city_id,
        city
    FROM
        film_src.city;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы store
CREATE OR REPLACE PROCEDURE staging.load_store()
AS
$$
BEGIN
    TRUNCATE TABLE staging.store;
    INSERT INTO staging.store (store_id, address_id)
    SELECT
        store_id,
        address_id
    FROM
        film_src.store;
END;
$$ LANGUAGE plpgsql;

--==================================================================================--
/*
                                          ////////////////////////////
                                         //          CORE          //
                                        ////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц Core-слоя
CREATE OR REPLACE PROCEDURE core.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS core.fact_payment;
    DROP TABLE IF EXISTS core.fact_rental;
    DROP TABLE IF EXISTS core.dim_inventory;
    DROP TABLE IF EXISTS core.dim_staff;
    DROP TABLE IF EXISTS core.dim_date;

    -- Таблица измерений по инвентарю
    CREATE TABLE core.dim_inventory
    (
        inventory_pk        SERIAL PRIMARY KEY,
        inventory_id        int4          NOT NULL,
        film_id             int4          NOT NULL,
        title               VARCHAR(255)  NOT NULL,
        rental_duration     int2          NOT NULL,
        rental_rate         NUMERIC(4, 2) NOT NULL,
        length              int2,
        rating              VARCHAR(10),
        effective_date_from TIMESTAMP     NOT NULL,
        effective_date_to   TIMESTAMP     NOT NULL,
        is_active           BOOLEAN       NOT NULL
    );

    -- Таблица измерений по персоналу
    CREATE TABLE core.dim_staff
    (
        staff_pk            SERIAL PRIMARY KEY,
        staff_id            int4        NOT NULL,
        first_name          VARCHAR(45) NOT NULL,
        last_name           VARCHAR(45) NOT NULL,
        address             VARCHAR(50) NOT NULL,
        district            VARCHAR(20) NOT NULL,
        city_name           VARCHAR(50) NOT NULL,
        effective_date_from TIMESTAMP   NOT NULL,
        effective_date_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL
    );

    -- Таблица измерений по периоду (Календарь)
    CREATE TABLE core.dim_date
    (
        date_pk                int4 PRIMARY KEY,
        date_actual            DATE        NOT NULL,
        day_name               VARCHAR(20) NOT NULL,
        day_of_week            int4        NOT NULL,
        day_of_month           int4        NOT NULL,
        day_of_quarter         int4        NOT NULL,
        day_of_year            int4        NOT NULL,
        week_of_month          int4        NOT NULL,
        week_of_year           int4        NOT NULL,
        month_actual           int4        NOT NULL,
        month_name             VARCHAR(20) NOT NULL,
        month_name_abbreviated VARCHAR(10) NOT NULL,
        quarter_actual         int4        NOT NULL,
        year_actual            int4        NOT NULL,
        first_day_of_week      DATE        NOT NULL,
        last_day_of_week       DATE        NOT NULL,
        first_day_of_month     DATE        NOT NULL,
        last_day_of_month      DATE        NOT NULL,
        first_day_of_quarter   DATE        NOT NULL,
        last_day_of_quarter    DATE        NOT NULL,
        first_day_of_year      DATE        NOT NULL,
        last_day_of_year       DATE        NOT NULL,
        mmyyyy                 VARCHAR(10) NOT NULL,
        mmddyyyy               VARCHAR(20) NOT NULL,
        weekend_indr           bool        NOT NULL
    );
    CREATE INDEX dim_date_date_actual_idx ON core.dim_date (date_actual);

    -- Таблица фактов по платежам
    CREATE TABLE core.fact_payment
    (
        payment_pk          SERIAL PRIMARY KEY,
        payment_id          int4          NOT NULL,
        rental_id           int4          NOT NULL,
        amount              NUMERIC(7, 2) NOT NULL,
        payment_date_fk     int4          NOT NULL REFERENCES core.dim_date (date_pk),
        inventory_fk        int4          NOT NULL REFERENCES core.dim_inventory (inventory_pk),
        staff_fk            int4          NOT NULL REFERENCES core.dim_staff (staff_pk),
        effective_date_from TIMESTAMP     NOT NULL,
        effective_date_to   TIMESTAMP     NOT NULL,
        is_active           BOOLEAN       NOT NULL
    );

    -- Таблица фактов по аренде (прокату)
    CREATE TABLE core.fact_rental
    (
        rental_pk           SERIAL PRIMARY KEY,
        rental_id           int4      NOT NULL,
        rental_date_fk      int4      NOT NULL REFERENCES core.dim_date (date_pk),
        return_date_fk      int4 REFERENCES core.dim_date (date_pk),
        inventory_fk        int4      NOT NULL REFERENCES core.dim_inventory (inventory_pk),
        staff_fk            int4      NOT NULL REFERENCES core.dim_staff (staff_pk),
        effective_date_from TIMESTAMP NOT NULL,
        effective_date_to   TIMESTAMP NOT NULL,
        is_active           BOOLEAN   NOT NULL
    );
END;
$$ LANGUAGE plpgsql;
--==================================================================================--


--=============================== ЗАПОЛНЕНИЕ ДАННЫМИ ===============================--
-- Процедура для заполнения таблицы dim_inventory
CREATE OR REPLACE PROCEDURE core.load__dim_inventory()
AS
$$
DECLARE
    var_film_prev_update TIMESTAMP;
BEGIN
    -- Помечаем удаленные записи
    UPDATE core.dim_inventory i
    SET
        is_active         = FALSE,
        effective_date_to = si.deleted
    FROM
        staging.inventory si
    WHERE
          si.deleted IS NOT NULL
      AND i.inventory_id = si.inventory_id
      AND i.is_active IS TRUE;

    -- Получаем список идентификаторов новых компакт-дисков
    CREATE TEMPORARY TABLE new_inventory_id_list ON COMMIT DROP AS
    SELECT
        i.inventory_id
    FROM
        staging.inventory i
            LEFT JOIN core.dim_inventory di USING (inventory_id)
    WHERE
        di.inventory_id IS NULL;

    -- Добавляем новые компакт-диски в измерение dim_inventory
    INSERT INTO
        core.dim_inventory (inventory_id, film_id, title, rental_duration, rental_rate, length, rating,
                            effective_date_from, effective_date_to, is_active)
    SELECT
        i.inventory_id,
        i.film_id,
        f.title,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.rating,
        '1900-01-01'::DATE                      AS effective_date_from,
        COALESCE(i.deleted, '2199-01-01'::DATE) AS effective_date_to,
        i.deleted IS NULL                       AS is_active
    FROM
        staging.inventory i
            JOIN staging.film f USING (film_id)
            JOIN new_inventory_id_list idl USING (inventory_id);

    -- Помечаем измененные компакт-диски неактивными
    UPDATE core.dim_inventory i
    SET
        is_active         = FALSE,
        effective_date_to = si.last_update
    FROM
        staging.inventory si
            LEFT JOIN new_inventory_id_list idl USING (inventory_id)
    WHERE
          idl.inventory_id IS NULL
      AND si.deleted IS NULL
      AND i.inventory_id = si.inventory_id
      AND i.is_active IS TRUE;

    -- По измененным компакт-дискам добавляем актуальные строки
    INSERT INTO
        core.dim_inventory (inventory_id, film_id, title, rental_duration, rental_rate, length, rating,
                            effective_date_from, effective_date_to, is_active)
    SELECT
        i.inventory_id,
        i.film_id,
        f.title,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.rating,
        i.last_update      AS effective_date_from,
        '2199-01-01'::DATE AS effective_date_to,
        TRUE               AS is_active
    FROM
        staging.inventory i
            JOIN staging.film f USING (film_id)
            LEFT JOIN new_inventory_id_list idl USING (inventory_id)
    WHERE
          idl.inventory_id IS NULL
      AND i.deleted IS NULL;

    -- Историчность по таблице film

    -- Получаем время предыдущей загрузки данных в staging.film, чтобы получить измененные фильмы
    var_film_prev_update = (WITH
                                lag_update AS (SELECT
                                                   LAG(lu.update_dt) OVER (ORDER BY lu.update_dt) AS lag_update_dt
                                               FROM
                                                   staging.last_update_data lu
                                               WHERE
                                                   lu.table_name = 'staging.film')
                            SELECT
                                MAX(lag_update_dt)
                            FROM
                                lag_update);

    -- Получаем список измененных фильмов с момента предыдущей загрузки
    CREATE TEMPORARY TABLE updated_films ON COMMIT DROP AS
    SELECT
        f.film_id,
        f.title,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.rating,
        f.last_update
    FROM
        staging.film f
    WHERE
        f.last_update >= var_film_prev_update;

    -- Строки в dim_inventory, которые необходимо поменять
    CREATE TEMPORARY TABLE dim_inventory_rows_to_update ON COMMIT DROP AS
    SELECT
        di.inventory_pk,
        uf.last_update
    FROM
        core.dim_inventory di
            JOIN updated_films uf ON uf.film_id = di.film_id
            AND uf.last_update > di.effective_date_from
            AND uf.last_update < di.effective_date_to;

    -- Вставляем строки с новыми значениями фильмов
    INSERT INTO
        core.dim_inventory (inventory_id, film_id, title, rental_duration, rental_rate, length, rating,
                            effective_date_from, effective_date_to, is_active)
    SELECT
        di.inventory_id,
        di.film_id,
        uf.title,
        uf.rental_duration,
        uf.rental_rate,
        uf.length,
        uf.rating,
        uf.last_update AS effective_date_from,
        di.effective_date_to,
        di.is_active
    FROM
        core.dim_inventory di
            JOIN dim_inventory_rows_to_update ru ON ru.inventory_pk = di.inventory_pk
            JOIN updated_films uf ON uf.film_id = di.film_id;

    -- Устанавливаем дату окончания действия строк для предыдущих параметров фильмов
    UPDATE core.dim_inventory di
    SET
        effective_date_to = ru.last_update,
        is_active         = FALSE
    FROM
        dim_inventory_rows_to_update ru
    WHERE
        ru.inventory_pk = di.inventory_pk;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_staff
CREATE OR REPLACE PROCEDURE core.load__dim_staff()
AS
$$
BEGIN
    -- Помечаем удаленные записи
    UPDATE core.dim_staff ds
    SET
        is_active         = FALSE,
        effective_date_to = s.deleted
    FROM
        staging.staff s
    WHERE
          s.deleted IS NOT NULL
      AND ds.staff_id = s.staff_id
      AND ds.is_active IS TRUE;

    -- Получаем список идентификаторов новых сотрудников
    CREATE TEMPORARY TABLE new_staff_id_list ON COMMIT DROP AS
    SELECT
        s.staff_id
    FROM
        staging.staff s
            LEFT JOIN core.dim_staff ds USING (staff_id)
    WHERE
        ds.staff_id IS NULL;

    -- Добавляем новых сотрудников в измерение dim_staff
    INSERT INTO
        core.dim_staff (staff_id, first_name, last_name, address, district, city_name, effective_date_from,
                        effective_date_to, is_active)
    SELECT
        s.staff_id,
        s.first_name,
        s.last_name,
        a.address,
        a.district,
        c.city                                  AS city_name,
        '1900-01-01'::DATE                      AS effective_date_from,
        COALESCE(s.deleted, '2199-01-01'::DATE) AS effective_date_to,
        s.deleted IS NULL                       AS is_active
    FROM
        staging.staff s
            JOIN staging.store st USING (store_id)
            JOIN staging.address a USING (address_id)
            JOIN staging.city c USING (city_id)
            JOIN new_staff_id_list idl USING (staff_id);

    -- Помечаем измененных сотрудников неактивными
    UPDATE core.dim_staff ds
    SET
        is_active         = FALSE,
        effective_date_to = s.last_update
    FROM
        staging.staff s
            LEFT JOIN new_staff_id_list idl USING (staff_id)
    WHERE
          idl.staff_id IS NULL
      AND s.deleted IS NULL
      AND ds.staff_id = s.staff_id
      AND ds.is_active IS TRUE;

    -- По измененным сотрудникам добавляем актуальные строки
    INSERT INTO
        core.dim_staff (staff_id, first_name, last_name, address, district, city_name, effective_date_from,
                        effective_date_to, is_active)
    SELECT
        s.staff_id,
        s.first_name,
        s.last_name,
        a.address,
        a.district,
        c.city             AS city_name,
        s.last_update      AS effective_date_from,
        '2199-01-01'::DATE AS effective_date_to,
        TRUE               AS is_active
    FROM
        staging.staff s
            JOIN staging.store st USING (store_id)
            JOIN staging.address a USING (address_id)
            JOIN staging.city c USING (city_id)
            LEFT JOIN new_staff_id_list idl USING (staff_id)
    WHERE
          idl.staff_id IS NULL
      AND s.deleted IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_date
CREATE OR REPLACE PROCEDURE core.load__dim_date(sdate DATE, nm INTEGER)
AS
$$
BEGIN
    SET lc_time = 'ru_RU';

    INSERT INTO core.dim_date
    SELECT
        TO_CHAR(datum, 'yyyymmdd')::INT                                   AS date_pk,
        datum                                                             AS date_actual,
        TO_CHAR(datum, 'TMDay')                                           AS day_name,
        EXTRACT(ISODOW FROM datum)                                        AS day_of_week,
        EXTRACT(DAY FROM datum)                                           AS day_of_month,
        datum - DATE_TRUNC('quarter', datum)::DATE + 1                    AS day_of_quarter,
        EXTRACT(DOY FROM datum)                                           AS day_of_year,
        TO_CHAR(datum, 'W')::INT                                          AS week_of_month,
        EXTRACT(WEEK FROM datum)                                          AS week_of_year,
        EXTRACT(MONTH FROM datum)                                         AS month_actual,
        TO_CHAR(datum, 'TMMonth')                                         AS month_name,
        TO_CHAR(datum, 'Mon')                                             AS month_name_abbreviated,
        EXTRACT(QUARTER FROM datum)                                       AS quarter_actual,
        EXTRACT(YEAR FROM datum)                                          AS year_actual,
        datum + (1 - EXTRACT(ISODOW FROM datum))::INT                     AS first_day_of_week,
        datum + (7 - EXTRACT(ISODOW FROM datum))::INT                     AS last_day_of_week,
        datum + (1 - EXTRACT(DAY FROM datum))::INT                        AS first_day_of_month,
        (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE   AS last_day_of_month,
        DATE_TRUNC('quarter', datum)::DATE                                AS first_day_of_quarter,
        (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
        TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD')       AS first_day_of_year,
        TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD')       AS last_day_of_year,
        TO_CHAR(datum, 'mmyyyy')                                          AS mmyyyy,
        TO_CHAR(datum, 'mmddyyyy')                                        AS mmddyyyy,

        CASE
            WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
            ELSE FALSE
            END                                                           AS weekend_indr
    FROM
        (SELECT
             sdate + SEQUENCE.DAY AS datum
         FROM
             GENERATE_SERIES(0, nm - 1) AS SEQUENCE (DAY)
         GROUP BY
             SEQUENCE.DAY) DQ
    ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы fact_payment
CREATE OR REPLACE PROCEDURE core.load__fact_payment()
AS
$$
BEGIN
    -- Отмечаем, что удаленные строки более не активны
    UPDATE core.fact_payment fp
    SET
        is_active         = FALSE,
        effective_date_to = p.deleted
    FROM
        staging.payment p
    WHERE
          p.deleted IS NOT NULL
      AND p.payment_id = fp.payment_id
      AND fp.is_active IS TRUE;

    -- Получаем список идентификаторов новых платежей
    CREATE TEMPORARY TABLE new_payment_id_list ON COMMIT DROP AS
    SELECT
        p.payment_id
    FROM
        staging.payment p
            LEFT JOIN core.fact_payment fp USING (payment_id)
    WHERE
        fp.payment_id IS NULL;

    -- Вставляем новые платежи
    INSERT INTO
        core.fact_payment (payment_id, rental_id, amount, payment_date_fk, inventory_fk, staff_fk, effective_date_from,
                           effective_date_to, is_active)
    SELECT
        p.payment_id,
        p.rental_id,
        p.amount,
        dd.date_pk                              AS payment_date_fk,
        di.inventory_pk                         AS inventory_fk,
        ds.staff_pk                             AS staff_fk,
        '1900-01-01'::DATE                      AS effective_date_from,
        COALESCE(p.deleted, '2199-01-01'::DATE) AS effective_date_to,
        p.deleted IS NULL                       AS is_active
    FROM
        staging.payment p
            JOIN new_payment_id_list np USING (payment_id)
            JOIN core.dim_inventory di ON di.inventory_id = p.inventory_id
            AND p.last_update BETWEEN di.effective_date_from AND di.effective_date_to
            JOIN core.dim_staff ds ON ds.staff_id = p.staff_id
            AND p.last_update BETWEEN ds.effective_date_from AND ds.effective_date_to
            JOIN core.dim_date dd ON dd.date_actual = p.payment_date::DATE;

    -- Получаем список платежей, по которым не было изменений по полям, по которым мы поддерживаем историчность
    CREATE TEMPORARY TABLE updated_payments_without_history ON COMMIT DROP AS
    SELECT
        p.payment_id
    FROM
        staging.payment p
            JOIN core.fact_payment fp ON fp.payment_id = p.payment_id
            AND p.last_update BETWEEN fp.effective_date_from AND fp.effective_date_to
            JOIN core.dim_date dd ON dd.date_pk = fp.payment_date_fk
    WHERE
          p.amount = fp.amount
      AND p.payment_date::DATE = dd.date_actual
      AND p.rental_id = fp.rental_id;

    -- Проставляем новые значения полей по измененным платежам, по которым не нужна историчность
    UPDATE core.fact_payment fp
    SET
        inventory_fk = di.inventory_pk,
        staff_fk     = ds.staff_pk
    FROM
        updated_payments_without_history upwh
            JOIN staging.payment p ON p.payment_id = upwh.payment_id
            JOIN core.dim_inventory di ON di.inventory_id = p.inventory_id
            AND p.last_update BETWEEN di.effective_date_from AND di.effective_date_to
            JOIN core.dim_staff ds ON ds.staff_id = p.staff_id
            AND p.last_update BETWEEN ds.effective_date_from AND ds.effective_date_to
    WHERE
          p.payment_id = fp.payment_id
      AND p.last_update BETWEEN fp.effective_date_from AND fp.effective_date_to;

    -- Помечаем платежи, по изменениям которых нужно реализовать историчность, не активными
    UPDATE core.fact_payment fp
    SET
        is_active         = FALSE,
        effective_date_to = p.last_update
    FROM
        staging.payment p
            LEFT JOIN updated_payments_without_history upwh ON upwh.payment_id = p.payment_id
            LEFT JOIN new_payment_id_list idl ON idl.payment_id = p.payment_id
    WHERE
          p.payment_id = fp.payment_id
      AND upwh.payment_id IS NULL
      AND idl.payment_id IS NULL
      AND fp.is_active IS TRUE
      AND p.deleted IS NULL;

    -- По измененным платежам, по которым нужна историчность, добавляем новые актуальные строки
    INSERT INTO
        core.fact_payment (payment_id, rental_id, amount, payment_date_fk, inventory_fk, staff_fk, effective_date_from,
                           effective_date_to, is_active)
    SELECT
        p.payment_id,
        p.rental_id,
        p.amount,
        dd.date_pk         AS payment_date_fk,
        di.inventory_pk    AS inventory_fk,
        ds.staff_pk        AS staff_fk,
        p.last_update      AS effective_date_from,
        '2199-01-01'::DATE AS effective_date_to,
        TRUE               AS is_active
    FROM
        staging.payment p
            LEFT JOIN updated_payments_without_history upwh ON upwh.payment_id = p.payment_id
            LEFT JOIN new_payment_id_list idl ON idl.payment_id = p.payment_id

            JOIN core.dim_inventory di ON di.inventory_id = p.inventory_id
            AND p.last_update BETWEEN di.effective_date_from AND di.effective_date_to
            JOIN core.dim_staff ds ON ds.staff_id = p.staff_id
            AND p.last_update BETWEEN ds.effective_date_from AND ds.effective_date_to
            JOIN core.dim_date dd ON dd.date_actual = p.payment_date::DATE
    WHERE
          upwh.payment_id IS NULL
      AND idl.payment_id IS NULL
      AND p.deleted IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы fact_rental
CREATE OR REPLACE PROCEDURE core.load__fact_rental()
AS
$$
BEGIN
    -- Отмечаем, что удаленные строки более не активны
    UPDATE core.fact_rental fr
    SET
        is_active         = FALSE,
        effective_date_to = r.deleted
    FROM
        staging.rental r
    WHERE
          r.deleted IS NOT NULL
      AND r.rental_id = fr.rental_id
      AND fr.is_active IS TRUE;

    -- Получаем список идентификаторов новых фактов сдачи в аренду
    CREATE TEMPORARY TABLE new_rental_id_list ON COMMIT DROP AS
    SELECT
        r.rental_id
    FROM
        staging.rental r
            LEFT JOIN core.fact_rental fr USING (rental_id)
    WHERE
        fr.rental_id IS NULL;

    -- Добавляем новые факты сдачи в аренду в таблицу fact_rental
    INSERT INTO
        core.fact_rental (rental_id, rental_date_fk, return_date_fk, inventory_fk, staff_fk, effective_date_from,
                          effective_date_to, is_active)
    SELECT
        r.rental_id,
        dd_ren.date_pk                          AS rental_date_fk,
        dd_ret.date_pk                          AS return_date_fk,
        di.inventory_pk                         AS inventory_fk,
        ds.staff_pk                             AS staff_fk,
        '1900-01-01'::DATE                      AS effective_date_from,
        COALESCE(r.deleted, '2199-01-01'::DATE) AS effective_date_to,
        r.deleted IS NULL                       AS is_active
    FROM
        staging.rental r
            JOIN new_rental_id_list idl ON idl.rental_id = r.rental_id
            JOIN core.dim_inventory di ON di.inventory_id = r.inventory_id
            AND r.last_update BETWEEN di.effective_date_from AND di.effective_date_to
            JOIN core.dim_staff ds ON ds.staff_id = r.staff_id
            AND r.last_update BETWEEN ds.effective_date_from AND ds.effective_date_to
            JOIN core.dim_date dd_ren ON dd_ren.date_actual = r.rental_date::DATE
            LEFT JOIN core.dim_date dd_ret ON dd_ret.date_actual = r.return_date::DATE;

    -- Получаем список фактов сдачи в аренду, по которым была только проставлена дата возврата
    CREATE TEMPORARY TABLE update_return_date_id_list ON COMMIT DROP AS
    SELECT
        r.rental_id
    FROM
        staging.rental r
            JOIN core.fact_rental fr USING (rental_id)
            JOIN core.dim_inventory di ON di.inventory_pk = fr.inventory_fk
            JOIN core.dim_staff ds ON ds.staff_pk = fr.staff_fk
            JOIN core.dim_date dd ON dd.date_pk = fr.rental_date_fk
            LEFT JOIN new_rental_id_list idl ON idl.rental_id = r.rental_id
    WHERE
          r.return_date IS NOT NULL
      AND fr.return_date_fk IS NULL
      AND fr.is_active IS TRUE
      AND di.inventory_id = r.inventory_id
      AND ds.staff_id = r.staff_id
      AND dd.date_actual = r.rental_date::DATE
      AND r.deleted IS NULL
      AND idl.rental_id IS NULL;

    -- Проставляем дату возврата у фактов сдачи в аренду, у которых была только проставлена дата возврата
    UPDATE core.fact_rental fr
    SET
        return_date_fk = dd.date_pk
    FROM
        staging.rental r
            JOIN update_return_date_id_list uidl USING (rental_id)
            JOIN core.dim_date dd ON dd.date_actual = r.return_date::DATE
    WHERE
          r.rental_id = fr.rental_id
      AND fr.is_active IS TRUE;

    -- Помечаем измененные факты сдачи в аренду не активными
    UPDATE core.fact_rental fr
    SET
        is_active         = FALSE,
        effective_date_to = r.last_update
    FROM
        staging.rental r
            LEFT JOIN update_return_date_id_list uidl USING (rental_id)
            LEFT JOIN new_rental_id_list idl USING (rental_id)
    WHERE
          r.rental_id = fr.rental_id
      AND uidl.rental_id IS NULL
      AND idl.rental_id IS NULL
      AND fr.is_active IS TRUE
      AND r.deleted IS NULL;

    -- По измененным фактам сдачи в аренду добавляем актуальные строки
    INSERT INTO
        core.fact_rental (rental_id, rental_date_fk, return_date_fk, inventory_fk, staff_fk, effective_date_from,
                          effective_date_to, is_active)
    SELECT
        r.rental_id,
        dd_ren.date_pk     AS rental_date_fk,
        dd_ret.date_pk     AS return_date_fk,
        di.inventory_pk    AS inventory_fk,
        ds.staff_pk        AS staff_fk,
        r.last_update      AS effective_date_from,
        '2199-01-01'::DATE AS effective_date_to,
        TRUE               AS is_active
    FROM
        staging.rental r
            JOIN core.dim_inventory di ON di.inventory_id = r.inventory_id
            AND r.last_update BETWEEN di.effective_date_from AND di.effective_date_to
            JOIN core.dim_staff ds ON ds.staff_id = r.staff_id
            AND r.last_update BETWEEN ds.effective_date_from AND ds.effective_date_to
            JOIN core.dim_date dd_ren ON dd_ren.date_actual = r.rental_date::DATE
            LEFT JOIN core.dim_date dd_ret ON dd_ret.date_actual = r.return_date::DATE
            LEFT JOIN new_rental_id_list idl ON idl.rental_id = r.rental_id
            LEFT JOIN update_return_date_id_list uidl ON uidl.rental_id = r.rental_id
    WHERE
          r.deleted IS NULL
      AND idl.rental_id IS NULL
      AND uidl.rental_id IS NULL;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--
/*
                                          ////////////////////////////
                                         //          MART          //
                                        ////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц Mart-слоя
CREATE OR REPLACE PROCEDURE mart.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS mart.sales_date;
    DROP TABLE IF EXISTS mart.sales_film;

    -- Таблица с суммой продаж за период
    CREATE TABLE mart.sales_date
    (
        date_title VARCHAR(20)   NOT NULL,
        date_sort  int4          NOT NULL,
        amount     NUMERIC(7, 2) NOT NULL
    );

    -- Таблица с суммой продаж по фильмам
    CREATE TABLE mart.sales_film
    (
        film_title VARCHAR(100)  NOT NULL,
        amount     NUMERIC(7, 2) NOT NULL
    );
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--=============================== ЗАПОЛНЕНИЕ ДАННЫМИ ===============================--
-- Процедура для заполнения таблицы sales_date
CREATE OR REPLACE PROCEDURE mart.load__sales_date()
AS
$$
BEGIN
    TRUNCATE TABLE mart.sales_date;
    INSERT INTO mart.sales_date (date_title, date_sort, amount)
    SELECT
        dd.day_of_month || ' ' || dd.month_name || ' ' || dd.year_actual AS date_title,
        dd.date_pk                                                       AS date_sort,
        SUM(fp.amount)                                                   AS amount
    FROM
        core.fact_payment fp
            JOIN core.dim_date dd ON dd.date_pk = fp.payment_date_fk
    GROUP BY 1, 2;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы sales_film
CREATE OR REPLACE PROCEDURE mart.load__sales_film()
AS
$$
BEGIN
    TRUNCATE TABLE mart.sales_film;
    INSERT INTO mart.sales_film (film_title, amount)
    SELECT
        di.title       AS film_title,
        SUM(fp.amount) AS amount
    FROM
        core.fact_payment fp
            JOIN core.dim_inventory di ON di.inventory_pk = fp.inventory_fk
    GROUP BY 1;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

-- Итоговая процедура по пересозданию всех таблиц
CREATE OR REPLACE PROCEDURE reset_full_data()
AS
$$
BEGIN
    CALL staging.create_all_tables(); -- При инкременте отпадает смысл в перезаливке таблиц
    CALL core.create_all_tables(); -- Таблицы надо динамически редактировать под условия DELETE, UPDATE, INSERT
    CALL mart.create_all_tables();
END;
$$ LANGUAGE plpgsql;


-- Итоговая процедура по загрузке и преобразованию данных
CREATE OR REPLACE PROCEDURE load_full_data()
AS
$$
DECLARE
    var_current_update_dt TIMESTAMP = NOW();
BEGIN
    -- staging
    CALL staging.load_film(var_current_update_dt);
    CALL staging.load_inventory(var_current_update_dt);
    CALL staging.load_staff(var_current_update_dt);
    CALL staging.load_rental(var_current_update_dt);
    CALL staging.load_payment(var_current_update_dt);
    CALL staging.load_address();
    CALL staging.load_city();
    CALL staging.load_store();
    -- core
    CALL core.load__dim_inventory();
    CALL core.load__dim_staff();
    CALL core.load__fact_payment();
    CALL core.load__fact_rental();
    -- mart
    CALL mart.load__sales_date();
    CALL mart.load__sales_film();
END;
$$ LANGUAGE plpgsql;

--============================ ВЫЗОВ ФИНАЛЬНЫХ ПРОЦЕДУР ===========================--
-- !Выполнять только когда требуется полная перезапись хранилища!
CALL reset_full_data();
CALL core.load__dim_date('2005-01-01'::DATE, 7305);
-- Календарь до конца 2024 года
-- !!!
--------------------------------------------------------------------------------------
CALL load_full_data();
--==================================================================================--
