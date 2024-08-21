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
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS integ;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;

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
    DROP TABLE IF EXISTS staging.address;
    DROP TABLE IF EXISTS staging.city;
    DROP TABLE IF EXISTS staging.staff;
    DROP TABLE IF EXISTS staging.store;
    DROP TABLE IF EXISTS staging.payment;
    DROP TABLE IF EXISTS staging.rental;

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

    -- Инвентарь (Диски)
    CREATE TABLE staging.inventory
    (
        inventory_id int4      NOT NULL,
        film_id      int2      NOT NULL,
        store_id     int2      NOT NULL,
        last_update  TIMESTAMP NOT NULL,
        deleted      TIMESTAMP NULL
    );

    -- Адреса
    CREATE TABLE staging.address
    (
        address_id  int4        NOT NULL,
        address     VARCHAR(50) NOT NULL,
        district    VARCHAR(20) NOT NULL,
        city_id     int2        NOT NULL,
        postal_code VARCHAR(10) NULL,
        phone       VARCHAR(20) NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Города
    CREATE TABLE staging.city
    (
        city_id     int4        NOT NULL,
        city        VARCHAR(50) NOT NULL,
        country_id  int2        NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Сотрудники
    CREATE TABLE staging.staff
    (
        staff_id    int4        NOT NULL,
        first_name  VARCHAR(45) NOT NULL,
        last_name   VARCHAR(45) NOT NULL,
        address_id  int2        NOT NULL,
        email       VARCHAR(50) NULL,
        store_id    int2        NOT NULL,
        username    VARCHAR(16) NOT NULL,
        last_update TIMESTAMP   NOT NULL,
        deleted     TIMESTAMP   NULL
    );

    -- Магазины
    CREATE TABLE staging.store
    (
        store_id         int4      NOT NULL,
        manager_staff_id int2      NOT NULL,
        address_id       int2      NOT NULL,
        last_update      TIMESTAMP NOT NULL
    );

    -- Оплата
    CREATE TABLE staging.payment
    (
        payment_id   int4          NOT NULL,
        customer_id  int2          NOT NULL,
        staff_id     int2          NOT NULL,
        rental_id    int4          NOT NULL,
        amount       NUMERIC(5, 2) NOT NULL,
        payment_date TIMESTAMP     NOT NULL,
        deleted      TIMESTAMP     NULL,
        last_update  TIMESTAMP     NOT NULL
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
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--================= СОЗДАНИЕ ДОП. ПРОЦЕДУР И ФУНКЦИЙ STAGING-СЛОЯ ==================--
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

--========================= ЗАПОЛНЕНИЕ ДАННЫМИ STAGING-СЛОЯ ==========================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE staging.upload_film(current_update_dt TIMESTAMP)
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
CREATE OR REPLACE PROCEDURE staging.upload_inventory(current_update_dt TIMESTAMP)
AS
$$
BEGIN
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
        film_src.inventory;

    CALL staging.set_table_load_time('staging.inventory', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы address
CREATE OR REPLACE PROCEDURE staging.upload_address(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.address;
    INSERT INTO
        staging.address (address_id, address, district, city_id, postal_code, phone, last_update)
    SELECT
        address_id,
        address,
        district,
        city_id,
        postal_code,
        phone,
        last_update
    FROM
        film_src.address;

    CALL staging.set_table_load_time('staging.address', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы city
CREATE OR REPLACE PROCEDURE staging.upload_city(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.city;
    INSERT INTO staging.city (city_id, city, country_id, last_update)
    SELECT
        city_id,
        city,
        country_id,
        last_update
    FROM
        film_src.city;

    CALL staging.set_table_load_time('staging.city', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы staff
CREATE OR REPLACE PROCEDURE staging.upload_staff(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.staff;
    INSERT INTO
        staging.staff (staff_id, first_name, last_name, address_id, email, store_id, username, last_update, deleted)
    SELECT
        staff_id,
        first_name,
        last_name,
        address_id,
        email,
        store_id,
        username,
        last_update,
        deleted
    FROM
        film_src.staff;

    CALL staging.set_table_load_time('staging.staff', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы store
CREATE OR REPLACE PROCEDURE staging.upload_store(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.store;
    INSERT INTO
        staging.store (store_id, manager_staff_id, address_id, last_update)
    SELECT
        store_id,
        manager_staff_id,
        address_id,
        last_update
    FROM
        film_src.store;

    CALL staging.set_table_load_time('staging.store', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы payment
CREATE OR REPLACE PROCEDURE staging.upload_payment(current_update_dt TIMESTAMP)
AS
$$
BEGIN
    TRUNCATE TABLE staging.payment;
    INSERT INTO
        staging.payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, deleted, last_update)
    SELECT
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        amount,
        payment_date,
        deleted,
        last_update
    FROM
        film_src.payment;

    CALL staging.set_table_load_time('staging.payment', current_update_dt);
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE staging.upload_rental(current_update_dt TIMESTAMP)
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
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //           ODS           //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц ODS-слоя
CREATE OR REPLACE PROCEDURE ods.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS ods.film;
    DROP TABLE IF EXISTS ods.inventory;
    DROP TABLE IF EXISTS ods.address;
    DROP TABLE IF EXISTS ods.city;
    DROP TABLE IF EXISTS ods.staff;
    DROP TABLE IF EXISTS ods.store;
    DROP TABLE IF EXISTS ods.payment;
    DROP TABLE IF EXISTS ods.rental;

    -- Фильмы
    CREATE TABLE ods.film
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

    -- Инвентарь (Диски)
    CREATE TABLE ods.inventory
    (
        inventory_id int4      NOT NULL,
        film_id      int2      NOT NULL,
        store_id     int2      NOT NULL,
        last_update  TIMESTAMP NOT NULL,
        deleted      TIMESTAMP NULL
    );

    -- Адреса
    CREATE TABLE ods.address
    (
        address_id  int4        NOT NULL,
        address     VARCHAR(50) NOT NULL,
        district    VARCHAR(20) NOT NULL,
        city_id     int2        NOT NULL,
        postal_code VARCHAR(10) NULL,
        phone       VARCHAR(20) NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Города
    CREATE TABLE ods.city
    (
        city_id     int4        NOT NULL,
        city        VARCHAR(50) NOT NULL,
        country_id  int2        NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Сотрудники
    CREATE TABLE ods.staff
    (
        staff_id    int4        NOT NULL,
        first_name  VARCHAR(45) NOT NULL,
        last_name   VARCHAR(45) NOT NULL,
        address_id  int2        NOT NULL,
        email       VARCHAR(50) NULL,
        store_id    int2        NOT NULL,
        username    VARCHAR(16) NOT NULL,
        last_update TIMESTAMP   NOT NULL,
        deleted     TIMESTAMP   NULL
    );

    -- Магазины
    CREATE TABLE ods.store
    (
        store_id         int4      NOT NULL,
        manager_staff_id int2      NOT NULL,
        address_id       int2      NOT NULL,
        last_update      TIMESTAMP NOT NULL
    );

    -- Оплата
    CREATE TABLE ods.payment
    (
        payment_id   int4          NOT NULL,
        customer_id  int2          NOT NULL,
        staff_id     int2          NOT NULL,
        rental_id    int4          NOT NULL,
        amount       NUMERIC(5, 2) NOT NULL,
        payment_date TIMESTAMP     NOT NULL,
        deleted      TIMESTAMP     NULL,
        last_update  TIMESTAMP     NOT NULL
    );

    -- Аренда (Прокат)
    CREATE TABLE ods.rental
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
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================== ЗАПОЛНЕНИЕ ДАННЫМИ ODS-СЛОЯ ===========================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_film()
AS
$$
BEGIN
    TRUNCATE TABLE ods.film;
    INSERT INTO
        ods.film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length,
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
        staging.film;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы inventory
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_inventory()
AS
$$
BEGIN
    TRUNCATE TABLE ods.inventory;
    INSERT INTO
        ods.inventory (inventory_id, film_id, store_id, last_update, deleted)
    SELECT
        inventory_id,
        film_id,
        store_id,
        last_update,
        deleted
    FROM
        staging.inventory;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы address
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_address()
AS
$$
BEGIN
    TRUNCATE TABLE ods.address;
    INSERT INTO
        ods.address (address_id, address, district, city_id, postal_code, phone, last_update)
    SELECT
        address_id,
        address,
        district,
        city_id,
        postal_code,
        phone,
        last_update
    FROM
        staging.address;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы city
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_city()
AS
$$
BEGIN
    TRUNCATE TABLE ods.city;
    INSERT INTO ods.city (city_id, city, country_id, last_update)
    SELECT
        city_id,
        city,
        country_id,
        last_update
    FROM
        staging.city;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы staff
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_staff()
AS
$$
BEGIN
    TRUNCATE TABLE ods.staff;
    INSERT INTO
        ods.staff (staff_id, first_name, last_name, address_id, email, store_id, username, last_update, deleted)
    SELECT
        staff_id,
        first_name,
        last_name,
        address_id,
        email,
        store_id,
        username,
        last_update,
        deleted
    FROM
        staging.staff;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы store
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_store()
AS
$$
BEGIN
    TRUNCATE TABLE ods.store;
    INSERT INTO
        ods.store (store_id, manager_staff_id, address_id, last_update)
    SELECT
        store_id,
        manager_staff_id,
        address_id,
        last_update
    FROM
        staging.store;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы payment
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_payment()
AS
$$
BEGIN
    DELETE
    FROM
        ods.payment odp
    WHERE
            odp.payment_id IN (SELECT
                                   sp.payment_id
                               FROM
                                   staging.payment sp);

    INSERT INTO
        ods.payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, deleted, last_update)
    SELECT
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        amount,
        payment_date,
        deleted,
        last_update
    FROM
        staging.payment;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE ods.preprocessed_load_rental()
AS
$$
BEGIN
    DELETE
    FROM
        ods.rental odr
    WHERE
            odr.rental_id IN (SELECT
                                  sr.rental_id
                              FROM
                                  staging.rental sr);

    INSERT INTO
        ods.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update, deleted)
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
        staging.rental;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //           REF           //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц REF-слоя
CREATE OR REPLACE PROCEDURE ref.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS ref.film;
    DROP TABLE IF EXISTS ref.inventory;
    DROP TABLE IF EXISTS ref.address;
    DROP TABLE IF EXISTS ref.city;
    DROP TABLE IF EXISTS ref.staff;
    DROP TABLE IF EXISTS ref.store;
    DROP TABLE IF EXISTS ref.payment;
    DROP TABLE IF EXISTS ref.rental;

    -- Фильмы
    CREATE TABLE ref.film
    (
        film_sk SERIAL NOT NULL,
        film_nk INT    NOT NULL
    );

    -- Инвентарь (Диски)
    CREATE TABLE ref.inventory
    (
        inventory_sk SERIAL NOT NULL,
        inventory_nk int4   NOT NULL
    );

    -- Адреса
    CREATE TABLE ref.address
    (
        address_sk SERIAL NOT NULL,
        address_nk int4   NOT NULL
    );

    -- Города
    CREATE TABLE ref.city
    (
        city_sk SERIAL NOT NULL,
        city_nk int4   NOT NULL
    );

    -- Сотрудники
    CREATE TABLE ref.staff
    (
        staff_sk SERIAL NOT NULL,
        staff_nk int4   NOT NULL
    );

    -- Магазины
    CREATE TABLE ref.store
    (
        store_sk SERIAL NOT NULL,
        store_nk int4   NOT NULL
    );

    -- Оплата
    CREATE TABLE ref.payment
    (
        payment_sk SERIAL NOT NULL,
        payment_nk int4   NOT NULL
    );

    -- Аренда (Прокат)
    CREATE TABLE ref.rental
    (
        rental_sk SERIAL NOT NULL,
        rental_nk int4   NOT NULL
    );
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================== ЗАПОЛНЕНИЕ ДАННЫМИ REF-СЛОЯ ===========================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE ref.sync_film_id()
AS
$$
BEGIN
    INSERT INTO ref.film (film_nk)
    SELECT
        f.film_id
    FROM
        ods.film f
            LEFT JOIN ref.film rf ON rf.film_nk = f.film_id
    WHERE
        rf.film_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы inventory
CREATE OR REPLACE PROCEDURE ref.sync_inventory_id()
AS
$$
BEGIN
    INSERT INTO ref.inventory (inventory_nk)
    SELECT
        i.inventory_id
    FROM
        ods.inventory i
            LEFT JOIN ref.inventory ri ON ri.inventory_nk = i.inventory_id
    WHERE
        ri.inventory_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы address
CREATE OR REPLACE PROCEDURE ref.sync_address_id()
AS
$$
BEGIN
    INSERT INTO ref.address (address_nk)
    SELECT
        a.address_id
    FROM
        ods.address a
            LEFT JOIN ref.address ra ON ra.address_nk = a.address_id
    WHERE
        ra.address_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы city
CREATE OR REPLACE PROCEDURE ref.sync_city_id()
AS
$$
BEGIN
    INSERT INTO ref.city (city_nk)
    SELECT
        c.city_id
    FROM
        ods.city c
            LEFT JOIN ref.city rc ON rc.city_nk = c.city_id
    WHERE
        rc.city_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы staff
CREATE OR REPLACE PROCEDURE ref.sync_staff_id()
AS
$$
BEGIN
    INSERT INTO ref.staff (staff_nk)
    SELECT
        s.staff_id
    FROM
        ods.staff s
            LEFT JOIN ref.staff rs ON rs.staff_nk = s.staff_id
    WHERE
        rs.staff_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы store
CREATE OR REPLACE PROCEDURE ref.sync_store_id()
AS
$$
BEGIN
    INSERT INTO ref.store (store_nk)
    SELECT
        s.store_id
    FROM
        ods.store s
            LEFT JOIN ref.store rs ON rs.store_nk = s.store_id
    WHERE
        rs.store_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы payment
CREATE OR REPLACE PROCEDURE ref.sync_payment_id()
AS
$$
BEGIN
    INSERT INTO ref.payment (payment_nk)
    SELECT
        p.payment_id
    FROM
        ods.payment p
            LEFT JOIN ref.payment rp ON rp.payment_nk = p.payment_id
    WHERE
        rp.payment_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE ref.sync_rental_id()
AS
$$
BEGIN
    INSERT INTO ref.rental (rental_nk)
    SELECT
        r.rental_id
    FROM
        ods.rental r
            LEFT JOIN ref.rental rr ON rr.rental_nk = r.rental_id
    WHERE
        rr.rental_nk IS NULL
    ORDER BY
        1;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //          INTEG          //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц Integration-слоя
CREATE OR REPLACE PROCEDURE integ.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS integ.film;
    DROP TABLE IF EXISTS integ.inventory;
    DROP TABLE IF EXISTS integ.address;
    DROP TABLE IF EXISTS integ.city;
    DROP TABLE IF EXISTS integ.staff;
    DROP TABLE IF EXISTS integ.store;
    DROP TABLE IF EXISTS integ.payment;
    DROP TABLE IF EXISTS integ.rental;

    -- Фильмы
    CREATE TABLE integ.film
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

    -- Инвентарь (Диски)
    CREATE TABLE integ.inventory
    (
        inventory_id int4      NOT NULL,
        film_id      int2      NOT NULL,
        store_id     int2      NOT NULL,
        last_update  TIMESTAMP NOT NULL,
        deleted      TIMESTAMP NULL
    );

    -- Адреса
    CREATE TABLE integ.address
    (
        address_id  int4        NOT NULL,
        address     VARCHAR(50) NOT NULL,
        district    VARCHAR(20) NOT NULL,
        city_id     int2        NOT NULL,
        postal_code VARCHAR(10) NULL,
        phone       VARCHAR(20) NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Города
    CREATE TABLE integ.city
    (
        city_id     int4        NOT NULL,
        city        VARCHAR(50) NOT NULL,
        country_id  int2        NOT NULL,
        last_update TIMESTAMP   NOT NULL
    );

    -- Сотрудники
    CREATE TABLE integ.staff
    (
        staff_id    int4        NOT NULL,
        first_name  VARCHAR(45) NOT NULL,
        last_name   VARCHAR(45) NOT NULL,
        address_id  int2        NOT NULL,
        email       VARCHAR(50) NULL,
        store_id    int2        NOT NULL,
        username    VARCHAR(16) NOT NULL,
        last_update TIMESTAMP   NOT NULL,
        deleted     TIMESTAMP   NULL
    );

    -- Магазины
    CREATE TABLE integ.store
    (
        store_id         int4      NOT NULL,
        manager_staff_id int2      NOT NULL,
        address_id       int2      NOT NULL,
        last_update      TIMESTAMP NOT NULL
    );

    -- Оплата
    CREATE TABLE integ.payment
    (
        payment_id   int4          NOT NULL,
        customer_id  int2          NOT NULL,
        staff_id     int2          NOT NULL,
        rental_id    int4          NOT NULL,
        amount       NUMERIC(5, 2) NOT NULL,
        payment_date TIMESTAMP     NOT NULL,
        deleted      TIMESTAMP     NULL,
        last_update  TIMESTAMP     NOT NULL
    );

    -- Аренда (Прокат)
    CREATE TABLE integ.rental
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
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================= ЗАПОЛНЕНИЕ ДАННЫМИ INTEG-СЛОЯ ==========================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE integ.processed_load_film()
AS
$$
BEGIN
    TRUNCATE TABLE integ.film;

    INSERT INTO
        integ.film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length,
                    replacement_cost, rating, last_update, special_features, fulltext)
    SELECT
        rf.film_sk AS film_id,
        f.title,
        f.description,
        f.release_year,
        f.language_id,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.replacement_cost,
        f.rating,
        f.last_update,
        f.special_features,
        f.fulltext
    FROM
        ods.film f
            JOIN ref.film rf ON rf.film_nk = f.film_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы inventory
CREATE OR REPLACE PROCEDURE integ.processed_load_inventory()
AS
$$
BEGIN
    TRUNCATE TABLE integ.inventory;

    INSERT INTO
        integ.inventory (inventory_id, film_id, store_id, last_update, deleted)
    SELECT
        ri.inventory_sk AS inventory_id,
        rf.film_sk      AS film_id,
        rs.store_sk     AS store_id,
        i.last_update,
        i.deleted
    FROM
        ods.inventory i
            JOIN ref.inventory ri ON ri.inventory_nk = i.inventory_id
            JOIN ref.film rf ON rf.film_nk = i.film_id
            JOIN ref.store rs ON rs.store_nk = i.store_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы address
CREATE OR REPLACE PROCEDURE integ.processed_load_address()
AS
$$
BEGIN
    TRUNCATE TABLE integ.address;

    INSERT INTO
        integ.address (address_id, address, district, city_id, postal_code, phone, last_update)
    SELECT
        ra.address_sk AS address_id,
        a.address,
        a.district,
        rc.city_sk    AS city_id,
        a.postal_code,
        a.phone,
        a.last_update
    FROM
        ods.address a
            JOIN ref.address ra ON ra.address_nk = a.address_id
            JOIN ref.city rc ON rc.city_nk = a.city_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы city
CREATE OR REPLACE PROCEDURE integ.processed_load_city()
AS
$$
BEGIN
    TRUNCATE TABLE integ.city;

    INSERT INTO integ.city (city_id, city, country_id, last_update)
    SELECT
        rc.city_sk AS city_id,
        c.city,
        c.country_id,
        c.last_update
    FROM
        ods.city c
            JOIN ref.city rc ON rc.city_nk = c.city_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы staff
CREATE OR REPLACE PROCEDURE integ.processed_load_staff()
AS
$$
BEGIN
    TRUNCATE TABLE integ.staff;

    INSERT INTO
        integ.staff (staff_id, first_name, last_name, address_id, email, store_id, username, last_update, deleted)
    SELECT
        rsf.staff_sk  AS staff_id,
        s.first_name,
        s.last_name,
        ra.address_sk AS address_id,
        s.email,
        rsr.store_sk  AS store_id,
        s.username,
        s.last_update,
        s.deleted
    FROM
        ods.staff s
            JOIN ref.staff rsf ON rsf.staff_nk = s.staff_id
            JOIN ref.address ra ON ra.address_nk = s.address_id
            JOIN ref.store rsr ON rsr.store_nk = s.store_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы store
CREATE OR REPLACE PROCEDURE integ.processed_load_store()
AS
$$
BEGIN
    TRUNCATE TABLE integ.store;

    INSERT INTO
        integ.store (store_id, manager_staff_id, address_id, last_update)
    SELECT
        rsr.store_sk  AS store_id,
        rsf.staff_sk  AS manager_staff_id,
        ra.address_sk AS address_id,
        s.last_update
    FROM
        ods.store s
            JOIN ref.store rsr ON rsr.store_nk = s.store_id
            JOIN ref.staff rsf ON rsf.staff_nk = s.manager_staff_id
            JOIN ref.address ra ON ra.address_nk = s.address_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы payment
CREATE OR REPLACE PROCEDURE integ.processed_load_payment()
AS
$$
DECLARE
    last_update_dt TIMESTAMP;

BEGIN
    -- Дата и время последней измененной записи, загруженной в предыдущий раз
    last_update_dt = (SELECT
                          COALESCE(MAX(last_update), '1900-01-01'::DATE)
                      FROM
                          integ.payment);

    -- Идентификаторы всех созданных, удаленных или измененных строк с предыдущей загрузки из ods в integ
    CREATE TEMPORARY TABLE updated_integ_pay_id_list ON COMMIT DROP AS
    SELECT
        payment_id
    FROM
        ods.payment
    WHERE
        last_update > last_update_dt;

    -- Удаляем из integ слоя все созданные, удаленные или измененные строки с предыдущей загрузки
    DELETE
    FROM
        integ.payment
    WHERE
            payment_id IN (SELECT
                               payment_id
                           FROM
                               updated_integ_pay_id_list);

    -- Вставляем в integ слой все созданные, удаленные или измененные строки с предыдущей загрузки
    INSERT INTO
        integ.payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, deleted, last_update)
    SELECT
        rp.payment_sk AS payment_id,
        p.customer_id,
        rs.staff_sk   AS staff_id,
        rr.rental_sk  AS rental_id,
        p.amount,
        p.payment_date,
        p.deleted,
        p.last_update
    FROM
        ods.payment p
            JOIN ref.payment rp ON rp.payment_nk = p.payment_id
            JOIN ref.staff rs ON rs.staff_nk = p.staff_id
            JOIN ref.rental rr ON rr.rental_nk = p.rental_id
            JOIN updated_integ_pay_id_list pil ON pil.payment_id = p.payment_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE integ.processed_load_rental()
AS
$$
DECLARE
    last_update_dt TIMESTAMP;

BEGIN
    -- Дата и время последней измененной записи, загруженной в предыдущий раз
    last_update_dt = (SELECT
                          COALESCE(MAX(last_update), '1900-01-01'::DATE)
                      FROM
                          integ.rental);

    -- Идентификаторы всех созданных, удаленных или измененных строк с предыдущей загрузки из ods в integ
    CREATE TEMPORARY TABLE updated_integ_rent_id_list ON COMMIT DROP AS
    SELECT
        rental_id
    FROM
        ods.rental
    WHERE
        last_update > last_update_dt;

    -- Удаляем из integ слоя все созданные, удаленные или измененные строки с предыдущей загрузки
    DELETE
    FROM
        integ.rental
    WHERE
            rental_id IN (SELECT
                              rental_id
                          FROM
                              updated_integ_rent_id_list);

    -- Вставляем в integ слой все созданные, удаленные или измененные строки с предыдущей загрузки
    INSERT INTO
        integ.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update, deleted)
    SELECT
        rr.rental_sk    AS rental_id,
        r.rental_date,
        ri.inventory_sk AS inventory_id,
        r.customer_id,
        r.return_date,
        rs.staff_sk     AS staff_id,
        r.last_update,
        r.deleted
    FROM
        ods.rental r
            JOIN ref.rental rr ON rr.rental_nk = r.rental_id
            JOIN ref.inventory ri ON ri.inventory_nk = r.inventory_id
            JOIN ref.staff rs ON rs.staff_nk = r.staff_id
            JOIN updated_integ_rent_id_list ril ON ril.rental_id = r.rental_id;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //           DDS           //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц DDS-слоя
CREATE OR REPLACE PROCEDURE dds.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS dds.dim_film;
    DROP TABLE IF EXISTS dds.dim_inventory;
    DROP TABLE IF EXISTS dds.dim_address;
    DROP TABLE IF EXISTS dds.dim_city;
    DROP TABLE IF EXISTS dds.dim_staff;
    DROP TABLE IF EXISTS dds.dim_store;
    DROP TABLE IF EXISTS dds.fact_payment;
    DROP TABLE IF EXISTS dds.fact_rental;

    -- Фильмы
    CREATE TABLE dds.dim_film
    (
        film_id             INT           NOT NULL,
        title               VARCHAR(255)  NOT NULL,
        description         TEXT          NULL,
        release_year        int2          NULL,
        language_id         int2          NOT NULL,
        rental_duration     int2          NOT NULL,
        rental_rate         NUMERIC(4, 2) NOT NULL,
        length              int2          NULL,
        replacement_cost    NUMERIC(5, 2) NOT NULL,
        rating              VARCHAR(10)   NULL,
        special_features    _text         NULL,
        fulltext            tsvector      NOT NULL,
        date_effective_from TIMESTAMP     NOT NULL,
        date_effective_to   TIMESTAMP     NOT NULL,
        is_active           BOOLEAN       NOT NULL,
        hash                VARCHAR(32)   NOT NULL
    );

    -- Инвентарь (Диски)
    CREATE TABLE dds.dim_inventory
    (
        inventory_id        int4        NOT NULL,
        film_id             int2        NOT NULL,
        store_id            int2        NOT NULL,
        date_effective_from TIMESTAMP   NOT NULL,
        date_effective_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL,
        hash                VARCHAR(32) NOT NULL
    );

    -- Адреса
    CREATE TABLE dds.dim_address
    (
        address_id          int4        NOT NULL,
        address             VARCHAR(50) NOT NULL,
        district            VARCHAR(20) NOT NULL,
        city_id             int2        NOT NULL,
        postal_code         VARCHAR(10) NULL,
        phone               VARCHAR(20) NOT NULL,
        date_effective_from TIMESTAMP   NOT NULL,
        date_effective_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL,
        hash                VARCHAR(32) NOT NULL
    );

    -- Города
    CREATE TABLE dds.dim_city
    (
        city_id             int4        NOT NULL,
        city                VARCHAR(50) NOT NULL,
        country_id          int2        NOT NULL,
        date_effective_from TIMESTAMP   NOT NULL,
        date_effective_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL,
        hash                VARCHAR(32) NOT NULL
    );

    -- Сотрудники
    CREATE TABLE dds.dim_staff
    (
        staff_id            int4        NOT NULL,
        first_name          VARCHAR(45) NOT NULL,
        last_name           VARCHAR(45) NOT NULL,
        address_id          int2        NOT NULL,
        email               VARCHAR(50) NULL,
        store_id            int2        NOT NULL,
        username            VARCHAR(16) NOT NULL,
        date_effective_from TIMESTAMP   NOT NULL,
        date_effective_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL,
        hash                VARCHAR(32) NOT NULL
    );

    -- Магазины
    CREATE TABLE dds.dim_store
    (
        store_id            int4        NOT NULL,
        manager_staff_id    int2        NOT NULL,
        address_id          int2        NOT NULL,
        date_effective_from TIMESTAMP   NOT NULL,
        date_effective_to   TIMESTAMP   NOT NULL,
        is_active           BOOLEAN     NOT NULL,
        hash                VARCHAR(32) NOT NULL
    );

    -- Оплата
    CREATE TABLE dds.fact_payment
    (
        payment_id   int4          NOT NULL,
        customer_id  int2          NOT NULL,
        staff_id     int2          NOT NULL,
        rental_id    int4          NOT NULL,
        amount       NUMERIC(5, 2) NOT NULL,
        payment_date TIMESTAMP     NOT NULL,
        deleted      TIMESTAMP     NULL,
        last_update  TIMESTAMP     NOT NULL
    );

    -- Аренда (Прокат)
    CREATE TABLE dds.fact_rental
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
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================= ЗАПОЛНЕНИЕ ДАННЫМИ DDS-СЛОЯ ==========================--
-- Процедура для заполнения таблицы dim_film
CREATE OR REPLACE PROCEDURE dds.load__dim_film()
AS
$$
BEGIN
    -- Список id новых фильмов
    CREATE TEMPORARY TABLE film_new_id_list ON COMMIT DROP AS
    SELECT
        f.film_sk AS film_id
    FROM
        ref.film f
            LEFT JOIN dds.dim_film df ON df.film_id = f.film_sk
    WHERE
        df.film_id IS NULL;

    -- Вставляем новые фильмы
    INSERT INTO
        dds.dim_film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length,
                      replacement_cost, rating, special_features, fulltext, date_effective_from, date_effective_to,
                      is_active, hash)
    SELECT
        f.film_id,
        f.title,
        f.description,
        f.release_year,
        f.language_id,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.replacement_cost,
        f.rating,
        f.special_features,
        f.fulltext,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(f::TEXT)       AS hash
    FROM
        integ.film f
            JOIN film_new_id_list il ON il.film_id = f.film_id;

    -- Находим id удаленных фильмов
    CREATE TEMPORARY TABLE film_deleted_id_list ON COMMIT DROP AS
    SELECT
        df.film_id
    FROM
        dds.dim_film df
            LEFT JOIN integ.film inf USING (film_id)
    WHERE
        inf.film_id IS NULL;

    -- Помечаем удаленные фильмы неактивными
    UPDATE dds.dim_film f
    SET
        is_active         = FALSE,
        date_effective_to = NOW()
    FROM
        film_deleted_id_list fdl
    WHERE
          fdl.film_id = f.film_id
      AND f.is_active IS TRUE;

    -- Находим id измененных фильмов
    CREATE TEMPORARY TABLE film_updated_id_list ON COMMIT DROP AS
    SELECT
        f.film_id
    FROM
        dds.dim_film df
            JOIN integ.film f ON f.film_id = df.film_id
    WHERE
          df.is_active IS TRUE
      AND df.hash <> MD5(f::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным фильмам
    UPDATE dds.dim_film df
    SET
        is_active         = FALSE,
        date_effective_to = f.last_update
    FROM
        integ.film f
            JOIN film_updated_id_list ul ON ul.film_id = f.film_id
    WHERE
          f.film_id = df.film_id
      AND df.is_active IS TRUE;

    -- Добавляем новые строки по измененным фильмам
    INSERT INTO
        dds.dim_film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length,
                      replacement_cost, rating, special_features, fulltext, date_effective_from, date_effective_to,
                      is_active, hash)
    SELECT
        f.film_id,
        f.title,
        f.description,
        f.release_year,
        f.language_id,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.replacement_cost,
        f.rating,
        f.special_features,
        f.fulltext,
        f.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(f::TEXT)       AS hash
    FROM
        integ.film f
            JOIN film_updated_id_list ul ON ul.film_id = f.film_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_inventory
CREATE OR REPLACE PROCEDURE dds.load__dim_inventory()
AS
$$
BEGIN
    -- Список id новых компакт-дисков
    CREATE TEMPORARY TABLE inventory_new_id_list ON COMMIT DROP AS
    SELECT
        i.inventory_sk AS inventory_id
    FROM
        ref.inventory i
            LEFT JOIN dds.dim_inventory di ON di.inventory_id = i.inventory_sk
    WHERE
        di.inventory_id IS NULL;

    -- Вставляем новые компакт-диски
    INSERT INTO
        dds.dim_inventory (inventory_id, film_id, store_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        i.inventory_id,
        i.film_id,
        i.store_id,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(i::TEXT)       AS hash
    FROM
        integ.inventory i
            JOIN inventory_new_id_list il ON il.inventory_id = i.inventory_id;

    -- Находим id удаленных компакт-дисков
    CREATE TEMPORARY TABLE inventory_deleted_id_list ON COMMIT DROP AS
    SELECT
        di.inventory_id,
        ii.deleted
    FROM
        dds.dim_inventory di
            LEFT JOIN integ.inventory ii USING (inventory_id)
    WHERE
        ii.inventory_id IS NULL;

    -- Помечаем удаленные компакт-диски неактивными
    UPDATE dds.dim_inventory i
    SET
        is_active         = FALSE,
        date_effective_to = idl.deleted
    FROM
        inventory_deleted_id_list idl
    WHERE
          idl.inventory_id = i.inventory_id
      AND i.is_active IS TRUE;

    -- Находим id измененных компакт-дисков
    CREATE TEMPORARY TABLE inventory_updated_id_list ON COMMIT DROP AS
    SELECT
        i.inventory_id
    FROM
        dds.dim_inventory di
            JOIN integ.inventory i ON i.inventory_id = di.inventory_id
    WHERE
          di.is_active IS TRUE
      AND di.hash <> MD5(i::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным компакт-дискам
    UPDATE dds.dim_inventory di
    SET
        is_active         = FALSE,
        date_effective_to = i.last_update
    FROM
        integ.inventory i
            JOIN inventory_updated_id_list iul ON iul.inventory_id = i.inventory_id
    WHERE
          i.inventory_id = di.inventory_id
      AND di.is_active IS TRUE;

    -- Добавляем новые строки по измененным компакт-дискам
    INSERT INTO
        dds.dim_inventory (inventory_id, film_id, store_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        i.inventory_id,
        i.film_id,
        i.store_id,
        i.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(i::TEXT)       AS hash
    FROM
        integ.inventory i
            JOIN inventory_updated_id_list iul ON iul.inventory_id = i.inventory_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_address
CREATE OR REPLACE PROCEDURE dds.load__dim_address()
AS
$$
BEGIN
    -- Список id новых адресов
    CREATE TEMPORARY TABLE address_new_id_list ON COMMIT DROP AS
    SELECT
        a.address_sk AS address_id
    FROM
        ref.address a
            LEFT JOIN dds.dim_address da ON da.address_id = a.address_sk
    WHERE
        da.address_id IS NULL;

    -- Вставляем новые адреса
    INSERT INTO
        dds.dim_address (address_id, address, district, city_id, postal_code, phone, date_effective_from,
                         date_effective_to, is_active, hash)
    SELECT
        a.address_id,
        a.address,
        a.district,
        a.city_id,
        a.postal_code,
        a.phone,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(a::TEXT)       AS hash
    FROM
        integ.address a
            JOIN address_new_id_list al ON al.address_id = a.address_id;

    -- Находим id удаленных адресов
    CREATE TEMPORARY TABLE address_deleted_id_list ON COMMIT DROP AS
    SELECT
        da.address_id
    FROM
        dds.dim_address da
            LEFT JOIN integ.address ia USING (address_id)
    WHERE
        ia.address_id IS NULL;

    -- Помечаем удаленные адреса неактивными
    UPDATE dds.dim_address a
    SET
        is_active         = FALSE,
        date_effective_to = NOW()
    FROM
        address_deleted_id_list adl
    WHERE
          adl.address_id = a.address_id
      AND a.is_active IS TRUE;

    -- Находим id измененных адресов
    CREATE TEMPORARY TABLE address_updated_id_list ON COMMIT DROP AS
    SELECT
        a.address_id
    FROM
        dds.dim_address da
            JOIN integ.address a ON a.address_id = da.address_id
    WHERE
          da.is_active IS TRUE
      AND da.hash <> MD5(a::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным адресам
    UPDATE dds.dim_address da
    SET
        is_active         = FALSE,
        date_effective_to = a.last_update
    FROM
        integ.address a
            JOIN address_updated_id_list aul ON aul.address_id = a.address_id
    WHERE
          a.address_id = da.address_id
      AND da.is_active IS TRUE;

    -- Добавляем новые строки по измененным адресам
    INSERT INTO
        dds.dim_address (address_id, address, district, city_id, postal_code, phone, date_effective_from,
                         date_effective_to, is_active, hash)
    SELECT
        a.address_id,
        a.address,
        a.district,
        a.city_id,
        a.postal_code,
        a.phone,
        a.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(a::TEXT)       AS hash
    FROM
        integ.address a
            JOIN address_updated_id_list aul ON aul.address_id = a.address_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_city
CREATE OR REPLACE PROCEDURE dds.load__dim_city()
AS
$$
BEGIN
    -- Список id новых городов
    CREATE TEMPORARY TABLE city_new_id_list ON COMMIT DROP AS
    SELECT
        c.city_sk AS city_id
    FROM
        ref.city c
            LEFT JOIN dds.dim_city dc ON dc.city_id = c.city_sk
    WHERE
        dc.city_id IS NULL;

    -- Вставляем новые города
    INSERT INTO
        dds.dim_city (city_id, city, country_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        c.city_id,
        c.city,
        c.country_id,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(c::TEXT)       AS hash
    FROM
        integ.city c
            JOIN city_new_id_list cl ON cl.city_id = c.city_id;

    -- Находим id удаленных городов
    CREATE TEMPORARY TABLE city_deleted_id_list ON COMMIT DROP AS
    SELECT
        dc.city_id
    FROM
        dds.dim_city dc
            LEFT JOIN integ.city ic USING (city_id)
    WHERE
        ic.city_id IS NULL;

    -- Помечаем удаленные города неактивными
    UPDATE dds.dim_city c
    SET
        is_active         = FALSE,
        date_effective_to = NOW()
    FROM
        city_deleted_id_list cdl
    WHERE
          cdl.city_id = c.city_id
      AND c.is_active IS TRUE;

    -- Находим id измененных городов
    CREATE TEMPORARY TABLE city_updated_id_list ON COMMIT DROP AS
    SELECT
        c.city_id
    FROM
        dds.dim_city dc
            JOIN integ.city c ON c.city_id = dc.city_id
    WHERE
          dc.is_active IS TRUE
      AND dc.hash <> MD5(c::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным городам
    UPDATE dds.dim_city dc
    SET
        is_active         = FALSE,
        date_effective_to = c.last_update
    FROM
        integ.city c
            JOIN city_updated_id_list cul ON cul.city_id = c.city_id
    WHERE
          c.city_id = dc.city_id
      AND dc.is_active IS TRUE;

    -- Добавляем новые строки по измененным городам
    INSERT INTO
        dds.dim_city (city_id, city, country_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        c.city_id,
        c.city,
        c.country_id,
        c.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(c::TEXT)       AS hash
    FROM
        integ.city c
            JOIN city_updated_id_list cul ON cul.city_id = c.city_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_staff
CREATE OR REPLACE PROCEDURE dds.load__dim_staff()
AS
$$
BEGIN
    -- Список id новых сотрудников
    CREATE TEMPORARY TABLE staff_new_id_list ON COMMIT DROP AS
    SELECT
        s.staff_sk AS staff_id
    FROM
        ref.staff s
            LEFT JOIN dds.dim_staff ds ON ds.staff_id = s.staff_sk
    WHERE
        ds.staff_id IS NULL;

    -- Вставляем новых сотрудников
    INSERT INTO
        dds.dim_staff (staff_id, first_name, last_name, address_id, email, store_id, username, date_effective_from,
                       date_effective_to, is_active, hash)
    SELECT
        s.staff_id,
        s.first_name,
        s.last_name,
        s.address_id,
        s.email,
        s.store_id,
        s.username,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(s::TEXT)       AS hash
    FROM
        integ.staff s
            JOIN staff_new_id_list sl ON sl.staff_id = s.staff_id;

    -- Находим id удаленных сотрудников
    CREATE TEMPORARY TABLE staff_deleted_id_list ON COMMIT DROP AS
    SELECT
        ds.staff_id,
        s.deleted
    FROM
        dds.dim_staff ds
            LEFT JOIN integ.staff s USING (staff_id)
    WHERE
        s.staff_id IS NULL;

    -- Помечаем удаленных сотрудников неактивными
    UPDATE dds.dim_staff s
    SET
        is_active         = FALSE,
        date_effective_to = sdl.deleted
    FROM
        staff_deleted_id_list sdl
    WHERE
          sdl.staff_id = s.staff_id
      AND s.is_active IS TRUE;

    -- Находим id измененных сотрудников
    CREATE TEMPORARY TABLE staff_updated_id_list ON COMMIT DROP AS
    SELECT
        s.staff_id
    FROM
        dds.dim_staff ds
            JOIN integ.staff s ON s.staff_id = ds.staff_id
    WHERE
          ds.is_active IS TRUE
      AND ds.hash <> MD5(s::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным сотрудникам
    UPDATE dds.dim_staff ds
    SET
        is_active         = FALSE,
        date_effective_to = s.last_update
    FROM
        integ.staff s
            JOIN staff_updated_id_list sul ON sul.staff_id = s.staff_id
    WHERE
          s.staff_id = ds.staff_id
      AND ds.is_active IS TRUE;

    -- Добавляем новые строки по измененным сотрудникам
    INSERT INTO
        dds.dim_staff (staff_id, first_name, last_name, address_id, email, store_id, username, date_effective_from,
                       date_effective_to, is_active, hash)
    SELECT
        s.staff_id,
        s.first_name,
        s.last_name,
        s.address_id,
        s.email,
        s.store_id,
        s.username,
        s.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(s::TEXT)       AS hash
    FROM
        integ.staff s
            JOIN staff_updated_id_list sul ON sul.staff_id = s.staff_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы dim_store
CREATE OR REPLACE PROCEDURE dds.load__dim_store()
AS
$$
BEGIN
    -- Список id новых магазинов
    CREATE TEMPORARY TABLE store_new_id_list ON COMMIT DROP AS
    SELECT
        s.store_sk AS store_id
    FROM
        ref.store s
            LEFT JOIN dds.dim_store ds ON ds.store_id = s.store_sk
    WHERE
        ds.store_id IS NULL;

    -- Вставляем новые магазины
    INSERT INTO
        dds.dim_store (store_id, manager_staff_id, address_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        s.store_id,
        s.manager_staff_id,
        s.address_id,
        '1900-01-01'::DATE AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(s::TEXT)       AS hash
    FROM
        integ.store s
            JOIN store_new_id_list sl ON sl.store_id = s.store_id;

    -- Находим id удаленных магазинов
    CREATE TEMPORARY TABLE store_deleted_id_list ON COMMIT DROP AS
    SELECT
        ds.store_id
    FROM
        dds.dim_store ds
            LEFT JOIN integ.store s USING (store_id)
    WHERE
        s.store_id IS NULL;

    -- Помечаем удаленные магазины неактивными
    UPDATE dds.dim_store s
    SET
        is_active         = FALSE,
        date_effective_to = NOW()
    FROM
        store_deleted_id_list sdl
    WHERE
          sdl.store_id = s.store_id
      AND s.is_active IS TRUE;

    -- Находим id измененных магазинов
    CREATE TEMPORARY TABLE store_updated_id_list ON COMMIT DROP AS
    SELECT
        s.store_id
    FROM
        dds.dim_store ds
            JOIN integ.store s ON s.store_id = ds.store_id
    WHERE
          ds.is_active IS TRUE
      AND ds.hash <> MD5(s::TEXT);

    -- Помечаем неактуальными предыдущие строки по измененным магазинам
    UPDATE dds.dim_store ds
    SET
        is_active         = FALSE,
        date_effective_to = s.last_update
    FROM
        integ.store s
            JOIN store_updated_id_list sul ON sul.store_id = s.store_id
    WHERE
          s.store_id = ds.store_id
      AND ds.is_active IS TRUE;

    -- Добавляем новые строки по измененным магазинам
    INSERT INTO
        dds.dim_store (store_id, manager_staff_id, address_id, date_effective_from, date_effective_to, is_active, hash)
    SELECT
        s.store_id,
        s.manager_staff_id,
        s.address_id,
        s.last_update      AS date_effective_from,
        '2199-01-01'::DATE AS date_effective_to,
        TRUE               AS is_active,
        MD5(s::TEXT)       AS hash
    FROM
        integ.store s
            JOIN store_updated_id_list sul ON sul.store_id = s.store_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы fact_payment
CREATE OR REPLACE PROCEDURE dds.load__fact_payment()
AS
$$
DECLARE
    last_update_dt TIMESTAMP;

BEGIN
    -- Дата и время последней измененной записи, загруженной в предыдущий раз
    last_update_dt = (SELECT
                          COALESCE(MAX(last_update), '1900-01-01'::DATE)
                      FROM
                          dds.fact_payment);

    -- Идентификаторы всех созданных, удаленных или измененных строк с предыдущей загрузки из integ в dds
    CREATE TEMPORARY TABLE updated_dds_pay_id_list ON COMMIT DROP AS
    SELECT
        payment_id
    FROM
        integ.payment
    WHERE
        last_update > last_update_dt;

    -- Удаляем из dds слоя все созданные, удаленные или измененные строки с предыдущей загрузки
    DELETE
    FROM
        dds.fact_payment
    WHERE
            payment_id IN (SELECT
                               payment_id
                           FROM
                               updated_dds_pay_id_list);

    -- Вставляем в integ слой все созданные, удаленные или измененные строки с предыдущей загрузки
    INSERT INTO
        dds.fact_payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, deleted, last_update)
    SELECT
        p.payment_id,
        p.customer_id,
        p.staff_id,
        p.rental_id,
        p.amount,
        p.payment_date,
        p.deleted,
        p.last_update
    FROM
        integ.payment p
            JOIN updated_dds_pay_id_list pil ON pil.payment_id = p.payment_id;
END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы fact_rental
CREATE OR REPLACE PROCEDURE dds.load__fact_rental()
AS
$$
DECLARE
    last_update_dt TIMESTAMP;

BEGIN
    -- Дата и время последней измененной записи, загруженной в предыдущий раз
    last_update_dt = (SELECT
                          COALESCE(MAX(last_update), '1900-01-01'::DATE)
                      FROM
                          dds.fact_rental);

    -- Идентификаторы всех созданных, удаленных или измененных строк с предыдущей загрузки из integ в dds
    CREATE TEMPORARY TABLE updated_dds_rent_id_list ON COMMIT DROP AS
    SELECT
        rental_id
    FROM
        integ.rental
    WHERE
        last_update > last_update_dt;

    -- Удаляем из dds слоя все созданные, удаленные или измененные строки с предыдущей загрузки
    DELETE
    FROM
        dds.fact_rental
    WHERE
            rental_id IN (SELECT
                              rental_id
                          FROM
                              updated_dds_rent_id_list);

    -- Вставляем в integ слой все созданные, удаленные или измененные строки с предыдущей загрузки
    INSERT INTO
        dds.fact_rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update, deleted)
    SELECT
        r.rental_id,
        r.rental_date,
        r.inventory_id,
        r.customer_id,
        r.return_date,
        r.staff_id,
        r.last_update,
        r.deleted
    FROM
        integ.rental r
            JOIN updated_dds_rent_id_list ril ON ril.rental_id = r.rental_id;
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //           MART          //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц MART-слоя
CREATE OR REPLACE PROCEDURE mart.create_all_tables()
AS
$$
BEGIN
    DROP TABLE IF EXISTS mart.calendar;
    DROP TABLE IF EXISTS mart.sales_by_date;
    DROP TABLE IF EXISTS mart.sales_by_film;

    -- Календарь
    CREATE TABLE mart.calendar
    (
        date_id      int4        NOT NULL,
        date_actual  DATE        NOT NULL,
        day_of_month int4        NOT NULL,
        month_name   VARCHAR(10) NOT NULL,
        year         int4        NOT NULL
    );

    -- Продажи по дням
    CREATE TABLE mart.sales_by_date
    (
        sales_date_id    int4        NOT NULL,
        sales_date_title VARCHAR(50) NOT NULL,
        amount           FLOAT       NOT NULL
    );

    -- Таблица с суммой продаж по фильмам
    CREATE TABLE mart.sales_by_film
    (
        film_title VARCHAR(100)  NOT NULL,
        amount     NUMERIC(7, 2) NOT NULL
    );
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================= ЗАПОЛНЕНИЕ ДАННЫМИ MART-СЛОЯ ==========================--
-- Процедура для заполнения календаря 
CREATE OR REPLACE PROCEDURE mart.fill_calendar(sdate DATE, nm INTEGER)
AS
$$
BEGIN
    SET lc_time = 'ru_RU';

    INSERT INTO mart.calendar
    SELECT
        TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
        datum                           AS date_actual,
        EXTRACT(DAY FROM datum)         AS day_of_month,
        TO_CHAR(datum, 'TMMonth')       AS month_name,
        EXTRACT(YEAR FROM datum)        AS year_actual
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

-- Процедура для заполнения таблицы продаж по дням 
CREATE OR REPLACE PROCEDURE mart.calc__sales_by_date()
AS
$$
BEGIN
    TRUNCATE TABLE mart.sales_by_date;

    INSERT INTO
        mart.sales_by_date (sales_date_id, sales_date_title, amount)
    SELECT
        c.date_id                                              AS sales_date_id,
        CONCAT(c.day_of_month, ' ', c.month_name, ' ', c.year) AS sales_date_title,
        SUM(p.amount)                                          AS amount
    FROM
        dds.fact_payment p
            JOIN mart.calendar c ON c.date_actual = p.payment_date::DATE
    WHERE
        p.deleted IS NULL
    GROUP BY
        c.date_id,
        c.day_of_month,
        c.month_name,
        c.year;
END;
$$ LANGUAGE plpgsql;

-- Функция, которая будет доступна пользователю для работы с таблицей продаж по дням
CREATE OR REPLACE FUNCTION mart.get_data_from_sales_by_date()
    RETURNS TABLE
            (
                sales_date_id    INT,
                sales_date_title VARCHAR,
                amount           FLOAT
            )
AS
$$
SELECT
    sales_date_id,
    sales_date_title,
    amount
FROM
    mart.sales_by_date
$$ LANGUAGE SQL;

-- Процедура для заполнения таблицы продаж по фильмам
CREATE OR REPLACE PROCEDURE mart.calc__sales_by_film()
AS
$$
BEGIN
    TRUNCATE TABLE mart.sales_by_film;

    INSERT INTO mart.sales_by_film (film_title, amount)
    SELECT
        f.title       AS film_title,
        SUM(p.amount) AS amount
    FROM
        dds.fact_payment p
            JOIN dds.fact_rental r ON r.rental_id = p.rental_id
            JOIN dds.dim_inventory i ON i.inventory_id = r.inventory_id
            JOIN dds.dim_film f ON f.film_id = i.film_id
    GROUP BY 1;
END;
$$ LANGUAGE plpgsql;

-- Функция, которая будет доступна пользователю для работы с таблицей продаж по фильмам
CREATE OR REPLACE FUNCTION mart.get_data_from_sales_by_film()
    RETURNS TABLE
            (
                film_title VARCHAR,
                amount     FLOAT
            )
AS
$$
SELECT
    film_title,
    amount
FROM
    mart.sales_by_film
$$ LANGUAGE SQL;
--==================================================================================--

--============================ ФИНАЛЬНЫЕ ПРОЦЕДУРЫ =================================--
-- Пересоздание всех таблиц
CREATE OR REPLACE PROCEDURE reset_full_data()
AS
$$
BEGIN
    CALL staging.create_all_tables();
    CALL ods.create_all_tables();
    CALL ref.create_all_tables();
    CALL integ.create_all_tables();
    CALL dds.create_all_tables();
    CALL mart.create_all_tables();
END;
$$ LANGUAGE plpgsql;

-- Загрузка данных в хранилище
CREATE OR REPLACE PROCEDURE load_full_data()
AS
$$
DECLARE
    var_current_update_dt TIMESTAMP = NOW();
BEGIN
    CALL staging.upload_film(var_current_update_dt);
    CALL staging.upload_inventory(var_current_update_dt);
    CALL staging.upload_address(var_current_update_dt);
    CALL staging.upload_city(var_current_update_dt);
    CALL staging.upload_staff(var_current_update_dt);
    CALL staging.upload_store(var_current_update_dt);
    CALL staging.upload_payment(var_current_update_dt);
    CALL staging.upload_rental(var_current_update_dt);

    CALL ods.preprocessed_load_film();
    CALL ods.preprocessed_load_inventory();
    CALL ods.preprocessed_load_address();
    CALL ods.preprocessed_load_city();
    CALL ods.preprocessed_load_staff();
    CALL ods.preprocessed_load_store();
    CALL ods.preprocessed_load_payment();
    CALL ods.preprocessed_load_rental();

    CALL ref.sync_film_id();
    CALL ref.sync_inventory_id();
    CALL ref.sync_address_id();
    CALL ref.sync_city_id();
    CALL ref.sync_staff_id();
    CALL ref.sync_store_id();
    CALL ref.sync_payment_id();
    CALL ref.sync_rental_id();

    CALL integ.processed_load_film();
    CALL integ.processed_load_inventory();
    CALL integ.processed_load_address();
    CALL integ.processed_load_city();
    CALL integ.processed_load_staff();
    CALL integ.processed_load_store();
    CALL integ.processed_load_payment();
    CALL integ.processed_load_rental();

    CALL dds.load__dim_film();
    CALL dds.load__dim_inventory();
    CALL dds.load__dim_address();
    CALL dds.load__dim_city();
    CALL dds.load__dim_staff();
    CALL dds.load__dim_store();
    CALL dds.load__fact_payment();
    CALL dds.load__fact_rental();

    CALL mart.calc__sales_by_date();
    CALL mart.calc__sales_by_film();
END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--============================ ВЫЗОВ ФИНАЛЬНЫХ ПРОЦЕДУР ============================--
-- !Выполнять только когда требуется полная перезапись хранилища!
CALL reset_full_data();
CALL mart.fill_calendar('2005-01-01'::DATE, 7305);
-- Календарь до конца 2024 года
--------- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ------------

-- Финальная процедура загрузки данных
CALL load_full_data();
--==================================================================================--

