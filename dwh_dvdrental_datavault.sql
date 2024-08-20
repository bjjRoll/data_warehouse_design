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

CREATE DOMAIN public."year" AS integer CHECK (VALUE >= 1901 AND VALUE <= 2155);

-- Импорт данных из базы источника в свою сырую схему
IMPORT FOREIGN SCHEMA public FROM SERVER film_pg INTO film_src;

-- Создание основных слоев (схем)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS edw;  
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
AS $$
    BEGIN 
        DROP TABLE IF EXISTS staging.last_update_data;
	DROP TABLE IF EXISTS staging.film;
	DROP TABLE IF EXISTS staging.inventory;
	DROP TABLE IF EXISTS staging.rental;
		
	-- Техническая таблица для инкрементальной загрузки данных из источника
	CREATE TABLE staging.last_update_data (
	    table_name varchar(100) NOT NULL,
	    update_dt  timestamp NOT NULL
	);
	
	-- Фильмы
	CREATE TABLE staging.film (
	    film_id                int NOT NULL,
	    title                  varchar(255) NOT NULL,
	    description            text NULL,
	    release_year           int2 NULL,
	    language_id            int2 NOT NULL,
	    rental_duration        int2 NOT NULL,
	    rental_rate            numeric(4, 2) NOT NULL,
	    length                 int2 NULL,
	    replacement_cost       numeric(5, 2) NOT NULL,
	    rating                 varchar(10) NULL,
	    last_update            timestamp NOT NULL,
	    special_features       _text NULL,
	    fulltext               tsvector NOT NULL,		
	    hub_film_hash_key      varchar(32) NOT NULL,
	    film_static_hash_diff  varchar(32) NOT NULL,
	    film_dynamic_hash_diff varchar(32) NOT NULL,
	    load_date              timestamp NOT NULL,
	    record_source          varchar(50) NOT NULL
	);

	-- Инвентарь (Диски)
	CREATE TABLE staging.inventory (
	    inventory_id                 int4 NOT NULL,
	    film_id                      int2 NOT NULL,
	    store_id                     int2 NOT NULL,
	    last_update                  timestamp NOT NULL,
	    deleted                      timestamp NULL,
	    hub_inventory_hash_key       varchar(32) NOT NULL,
	    hub_film_hash_key            varchar(32) NOT NULL,
	    link_film_inventory_hash_key varchar(32) NOT NULL,
	    load_date                    timestamp NOT NULL,
	    record_source                varchar(50) NOT NULL
	);
	
	-- Аренда (Прокат)
	CREATE TABLE staging.rental (
	    rental_id                      int4 NOT NULL,
	    rental_date                    timestamp NOT NULL,
	    inventory_id                   int4 NOT NULL,
	    customer_id                    int2 NOT NULL,
	    return_date                    timestamp,
	    staff_id                       int2 NOT NULL,
	    last_update                    timestamp NOT NULL,
	    deleted		                   timestamp NULL,
	    hub_rental_hash_key            varchar(32) NOT NULL,
	    hub_inventory_hash_key         varchar(32) NOT NULL,
	    link_rental_inventory_hash_key varchar(32) NOT NULL,
	    load_date                      timestamp NOT NULL,
	    record_source                  varchar(50) NOT NULL
	);
    END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--================= СОЗДАНИЕ ДОП. ПРОЦЕДУР И ФУНКЦИЙ STAGING-СЛОЯ ==================--
-- Функция, возвращающая время последней загрузки в таблицы
CREATE OR REPLACE FUNCTION staging.get_last_update_table(table_name varchar) RETURNS timestamp
AS $$
    BEGIN 
        RETURN coalesce(
	    (
	      SELECT 
	          max(update_dt)
	      FROM 
	          staging.last_update_data lu 
	      WHERE 
		  lu.table_name = get_last_update_table.table_name
	    ),
				
	    '1900-01-01'::date
	);
    END;	
$$ LANGUAGE plpgsql;

-- Процедура вставки данных в last_update_data, в которой содержится информация по времени загрузки данных в конкретную таблицу
CREATE OR REPLACE PROCEDURE staging.set_table_load_time(table_name varchar, current_update_dt timestamp DEFAULT now()) 
AS $$
    BEGIN
        INSERT INTO staging.last_update_data (table_name, update_dt)
	VALUES (table_name, current_update_dt);
    END;
$$ LANGUAGE plpgsql;

--========================= ЗАПОЛНЕНИЕ ДАННЫМИ STAGING-СЛОЯ ==========================--
-- Процедура для заполнения таблицы film
CREATE OR REPLACE PROCEDURE staging.upload_film(current_update_dt timestamp)
AS $$
    BEGIN
        TRUNCATE TABLE staging.film;
	INSERT INTO staging.film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext, hub_film_hash_key, film_static_hash_diff, film_dynamic_hash_diff, load_date, record_source)
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
	    fulltext,
	    upper(md5(upper(trim(coalesce(film_id::text, '')))))    AS hub_film_hash_key,
	    upper(md5(upper(
	         concat(
		         trim(coalesce(title::text, '')), ';',				
		         trim(coalesce(description::text, '')), ';',
		         trim(coalesce(release_year::text, '')), ';',
		         trim(coalesce(length::text, '')), ';',
		         trim(coalesce(rating::text, ''))
		       )
	    )))                                                     AS film_static_hash_diff, 
	    upper(md5(upper(
	         concat(
		         trim(coalesce(rental_duration::text, '')), ';',				
		         trim(coalesce(rental_rate::text, '')), ';',
		         trim(coalesce(replacement_cost::text, ''))
		       )
	    )))                                                     AS film_dynamic_hash_diff,
	    current_update_dt                                       AS load_date,
	    'dvdrental_db'                                          AS record_source
	FROM 
	    film_src.film;
	
	CALL staging.set_table_load_time('staging.film', current_update_dt);
    END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы inventory
CREATE OR REPLACE PROCEDURE staging.upload_inventory(current_update_dt timestamp)
AS $$
    BEGIN
        TRUNCATE TABLE staging.inventory;
	INSERT INTO staging.inventory (inventory_id, film_id, store_id, last_update, deleted, hub_inventory_hash_key, hub_film_hash_key, link_film_inventory_hash_key, load_date, record_source)
	SELECT 
	    inventory_id, 
	    film_id, 
	    store_id,
	    last_update,
	    deleted,
	    upper(md5(upper(trim(coalesce(inventory_id::text, ''))))) AS hub_inventory_hash_key,
	    upper(md5(upper(trim(coalesce(film_id::text, '')))))      AS hub_film_hash_key,
	    upper(md5(upper(
                 concat(
			 trim(coalesce(film_id::text, '')), ';',
			 trim(coalesce(inventory_id::text, ''))
		       )
	    )))                                                       AS link_film_inventory_hash_key,
	    current_update_dt                                         AS load_date,
	    'dvdrental_db'                                            AS record_source
	FROM 
	    film_src.inventory;
		
	CALL staging.set_table_load_time('staging.inventory', current_update_dt);
    END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения таблицы rental
CREATE OR REPLACE PROCEDURE staging.upload_rental(current_update_dt timestamp)
AS $$
    DECLARE 
        var_last_update_dt timestamp;
    BEGIN
        var_last_update_dt = staging.get_last_update_table('staging.rental');
 
        TRUNCATE TABLE staging.rental;
        INSERT INTO staging.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update, deleted, hub_rental_hash_key, hub_inventory_hash_key, link_rental_inventory_hash_key, load_date, record_source)
        SELECT 
            rental_id, 
	    rental_date, 
	    inventory_id, 
	    customer_id, 
	    return_date, 
	    staff_id,
	    last_update,
	    deleted,
	    upper(md5(upper(trim(coalesce(rental_id::text, '')))))    AS hub_rental_hash_key, 
	    upper(md5(upper(trim(coalesce(inventory_id::text, ''))))) AS hub_inventory_hash_key, 
	    upper(md5(upper(
	         concat(
		         trim(coalesce(rental_id::text, '')), ';',
		         trim(coalesce(inventory_id::text, ''))
		       )
	    )))                                                       AS link_rental_inventory_hash_key,
	    current_update_dt                                         AS load_date,
	    'dvdrental_db'                                            AS record_source
        FROM 
            film_src.rental
        WHERE 
            last_update >= var_last_update_dt OR 
	    deleted >= var_last_update_dt;
	
        CALL staging.set_table_load_time('staging.rental', current_update_dt);
    END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--==================================================================================--
/*
                                          /////////////////////////////
                                         //           EDW           //
                                        /////////////////////////////
*/
--=========================== УДАЛЕНИЕ И СОЗДАНИЕ ТАБЛИЦ ===========================--
-- Процедура по созданию таблиц EDW-слоя
CREATE OR REPLACE PROCEDURE edw.create_all_tables()
AS $$
	BEGIN 	  	
	    DROP TABLE IF EXISTS edw.sat_film_static;
		DROP TABLE IF EXISTS edw.sat_film_dynamic;
	    DROP TABLE IF EXISTS edw.sat_film_inventory;
		DROP TABLE IF EXISTS edw.sat_inventory;
	    DROP TABLE IF EXISTS edw.sat_rental_inventory;
		DROP TABLE IF EXISTS edw.sat_rental_static;
		DROP TABLE IF EXISTS edw.sat_rental_dynamic;
	
		DROP TABLE IF EXISTS edw.link_film_inventory;
		DROP TABLE IF EXISTS edw.link_rental_inventory;
	
		DROP TABLE IF EXISTS edw.hub_film;
		DROP TABLE IF EXISTS edw.hub_inventory;
	    DROP TABLE IF EXISTS edw.hub_rental;
	
		-- Создание хабов:
		CREATE TABLE edw.hub_film (
			hub_film_hash_key varchar(32) PRIMARY KEY,
			load_date         timestamp NOT NULL,
			record_source     varchar(50) NOT NULL,
			film_id           int4 NOT NULL
		);

		CREATE TABLE edw.hub_inventory (
			hub_inventory_hash_key varchar(32) PRIMARY KEY,
			load_date              timestamp NOT NULL,
			record_source          varchar(50) NOT NULL,
			inventory_id           int4 NOT NULL
		);
	
		CREATE TABLE edw.hub_rental (
			hub_rental_hash_key varchar(32) PRIMARY KEY,
			load_date           timestamp NOT NULL,
			record_source       varchar(50) NOT NULL,
			rental_id           int4 NOT NULL
		);
	
		-- Создание линков:
		CREATE TABLE edw.link_film_inventory (
			link_film_inventory_hash_key varchar(32) PRIMARY KEY,
			load_date                    timestamp NOT NULL,
			record_source                varchar(50) NOT NULL,
			hub_film_hash_key            varchar(32) REFERENCES edw.hub_film (hub_film_hash_key),
			hub_inventory_hash_key       varchar(32) REFERENCES edw.hub_inventory (hub_inventory_hash_key)
		);

		CREATE TABLE edw.link_rental_inventory (
			link_rental_inventory_hash_key varchar(32) PRIMARY KEY,
			load_date                      timestamp NOT NULL,
			record_source                  varchar(50) NOT NULL,
			hub_rental_hash_key            varchar(32) REFERENCES edw.hub_rental (hub_rental_hash_key),
			hub_inventory_hash_key         varchar(32) REFERENCES edw.hub_inventory (hub_inventory_hash_key)
		);
	
		-- Создание саттелитов:
		CREATE TABLE edw.sat_film_static (
			hub_film_hash_key varchar(32) NOT NULL REFERENCES edw.hub_film (hub_film_hash_key),
			load_date         timestamp NOT NULL,
			load_end_date     timestamp NULL,
			record_source     varchar(50) NOT NULL,
			hash_diff         varchar(32) NOT NULL,
			
			title            varchar(255) NULL,
			description      text NULL,
			release_year     int2 NULL,
			length           int2 NULL,
			rating           varchar(10) NULL,
			
			PRIMARY KEY (hub_film_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_film_dynamic (
			hub_film_hash_key varchar(32) NOT NULL REFERENCES edw.hub_film (hub_film_hash_key),
			load_date         timestamp NOT NULL,
			load_end_date     timestamp NULL,
			record_source     varchar(50) NOT NULL,
			hash_diff         varchar(32) NOT NULL,
			
			rental_duration   int2 NULL,
			rental_rate       numeric(4, 2) NULL,
			replacement_cost  numeric(5, 2) NULL,
			
			PRIMARY KEY (hub_film_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_film_inventory (
			link_film_inventory_hash_key varchar(32) NOT NULL REFERENCES edw.link_film_inventory (link_film_inventory_hash_key),
			load_date                    timestamp NOT NULL,
			load_end_date                timestamp NULL,
			record_source                varchar(50) NOT NULL,
			
			PRIMARY KEY (link_film_inventory_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_inventory (
			hub_inventory_hash_key varchar(32) NOT NULL REFERENCES edw.hub_inventory (hub_inventory_hash_key),
			load_date              timestamp NOT NULL,
			load_end_date          timestamp NULL,
			record_source          varchar(50) NOT NULL,
			
			PRIMARY KEY (hub_inventory_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_rental_inventory (
			link_rental_inventory_hash_key varchar(32) NOT NULL REFERENCES edw.link_rental_inventory (link_rental_inventory_hash_key),
			load_date                      timestamp NOT NULL,
			load_end_date                  timestamp NULL,
			record_source                  varchar(50) NOT NULL,
			
			PRIMARY KEY (link_rental_inventory_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_rental_static (
			hub_rental_hash_key varchar(32) NOT NULL REFERENCES edw.hub_rental (hub_rental_hash_key),
			load_date           timestamp NOT NULL,
			load_end_date       timestamp NULL,
			record_source       varchar(50) NOT NULL,
			
			rental_date         timestamp NULL,
			
			PRIMARY KEY (hub_rental_hash_key, load_date)
		);
	
		CREATE TABLE edw.sat_rental_dynamic (
			hub_rental_hash_key varchar(32) NOT NULL REFERENCES edw.hub_rental (hub_rental_hash_key),
			load_date           timestamp NOT NULL,
			load_end_date       timestamp NULL,
			record_source       varchar(50) NOT NULL,
			
			return_date         timestamp NULL,
			
			PRIMARY KEY (hub_rental_hash_key, load_date)
		);
	END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--========================== ЗАПОЛНЕНИЕ ДАННЫМИ EDW-СЛОЯ ===========================--
-- ХАБЫ:
-- Процедура для заполнения хаба "фильмов"
CREATE OR REPLACE PROCEDURE edw.load_hub_film()
AS $$
	BEGIN 
		INSERT INTO edw.hub_film (hub_film_hash_key, load_date, record_source, film_id)
		SELECT 
			q.hub_film_hash_key, 
			q.load_date, 
			q.record_source, 
			q.film_id
		FROM 
			(
				SELECT
					f.hub_film_hash_key, 
					f.load_date, 
					f.record_source, 
					f.film_id
				FROM 
					staging.film f
				WHERE 
					f.film_id NOT IN (
						SELECT
							film_id
						FROM 
							edw.hub_film
					)
		
				UNION 
		
				SELECT
					i.hub_film_hash_key, 
					i.load_date, 
					i.record_source, 
					i.film_id
				FROM 
					staging.inventory i
				WHERE 
					i.film_id NOT IN (
						SELECT
							film_id
						FROM 
							edw.hub_film
					)
			) q;
	END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения хаба "компакт-дисков"
CREATE OR REPLACE PROCEDURE edw.load_hub_inventory()
AS $$
	BEGIN 
		INSERT INTO edw.hub_inventory (hub_inventory_hash_key, load_date, record_source, inventory_id)
		SELECT 
			q.hub_inventory_hash_key, 
			q.load_date, 
			q.record_source, 
			q.inventory_id
		FROM 
			(
				SELECT
					i.hub_inventory_hash_key, 
					i.load_date, 
					i.record_source, 
					i.inventory_id
				FROM 
					staging.inventory i
				WHERE 
					i.inventory_id NOT IN (
						SELECT
							inventory_id
						FROM 
							edw.hub_inventory
					)
		
				UNION 
		
				SELECT
					r.hub_inventory_hash_key, 
					r.load_date, 
					r.record_source, 
					r.inventory_id
				FROM 
					staging.rental r
				WHERE 
					r.inventory_id NOT IN (
						SELECT
							inventory_id
						FROM 
							edw.hub_inventory
					)
			) q;
	END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения хаба "фактов сдачи в аренду"
CREATE OR REPLACE PROCEDURE edw.load_hub_rental()
AS $$
	BEGIN 
		INSERT INTO edw.hub_rental (hub_rental_hash_key, load_date, record_source, rental_id)
		SELECT 
			q.hub_rental_hash_key, 
			q.load_date, 
			q.record_source, 
			q.rental_id
		FROM 
			(
				SELECT
					r.hub_rental_hash_key, 
					r.load_date, 
					r.record_source, 
					r.rental_id
				FROM 
					staging.rental r
				WHERE 
					r.rental_id NOT IN (
						SELECT
							rental_id
						FROM 
							edw.hub_rental
					)
			) q;
	END;
$$ LANGUAGE plpgsql;

-- ЛИНКИ:
-- Процедура для заполнения линка "фильмы/компакт-диски"
CREATE OR REPLACE PROCEDURE edw.load_link_film_inventory()
AS $$
	BEGIN 
		INSERT INTO edw.link_film_inventory (link_film_inventory_hash_key, load_date, record_source, hub_film_hash_key, hub_inventory_hash_key)
		SELECT 
			i.link_film_inventory_hash_key,
			i.load_date,
			i.record_source,
			i.hub_film_hash_key,
			i.hub_inventory_hash_key
		FROM 
			staging.inventory i
		WHERE 
			NOT EXISTS (
				SELECT
					1
				FROM 
					edw.link_film_inventory lfi
				WHERE 
					lfi.link_film_inventory_hash_key = i.link_film_inventory_hash_key 
			);
	END;
$$ LANGUAGE plpgsql;

-- Процедура для заполнения линка "факты сдачи в аренду/компакт-диски"
CREATE OR REPLACE PROCEDURE edw.load_link_rental_inventory()
AS $$
	BEGIN 
		INSERT INTO edw.link_rental_inventory (link_rental_inventory_hash_key, load_date, record_source, hub_rental_hash_key, hub_inventory_hash_key)
		SELECT 
			r.link_rental_inventory_hash_key, 
			r.load_date, 
			r.record_source, 
			r.hub_rental_hash_key, 
			r.hub_inventory_hash_key 
		FROM 
			staging.rental r
		WHERE 
			NOT EXISTS (
				SELECT
					1
				FROM 
					edw.link_rental_inventory lri
				WHERE 
					lri.link_rental_inventory_hash_key = r.link_rental_inventory_hash_key 
			);
	END;
$$ LANGUAGE plpgsql;

-- САТТЕЛИТЫ:
-- Процедура для заполнения саттелита "неизменная информация по фильмам"
CREATE OR REPLACE PROCEDURE edw.load_sat_film_static(current_update_dt timestamp)
AS $$
	BEGIN 
		-- Добавляем новые строки по фильмам
		INSERT INTO edw.sat_film_static (hub_film_hash_key, load_date, load_end_date, record_source, hash_diff, title, description, release_year, length, rating)
		SELECT 
			f.hub_film_hash_key,
			f.load_date,
			NULL                     AS load_end_date,
			f.record_source,
			f.film_static_hash_diff,
			f.title,
			f.description,
			f.release_year,
			f.length,
			f.rating
		FROM 
			staging.film f
			LEFT JOIN edw.sat_film_static sf ON sf.hub_film_hash_key = f.hub_film_hash_key AND sf.load_end_date IS NULL 
		WHERE 
			sf.hub_film_hash_key IS NULL OR 
			sf.hash_diff <> f.film_static_hash_diff;
		
		-- Помечаем неактуальные строки
		WITH updated_sat AS (
			SELECT 
				sf1.hub_film_hash_key,
				sf1.load_date,
				sf2.load_date          AS load_end_date 
			FROM 
				edw.sat_film_static sf1
				JOIN edw.sat_film_static sf2 ON sf2.hub_film_hash_key = sf1.hub_film_hash_key
					AND sf2.load_date > sf1.load_date 
					AND sf1.load_end_date IS NULL 
					AND sf2.load_end_date IS NULL 
		)
		UPDATE edw.sat_film_static AS sf 
		SET 
			load_end_date = us.load_end_date
		FROM 
			updated_sat us
		WHERE 
			sf.hub_film_hash_key = us.hub_film_hash_key AND 
			sf.load_date = us.load_date;
		
		-- Помечаем удаленные строки
		WITH deleted_sat AS (
			SELECT 
				sf.hub_film_hash_key,
				sf.load_date,
				current_update_dt     AS load_end_date 
			FROM 
				edw.sat_film_static sf
				LEFT JOIN staging.film f ON f.hub_film_hash_key = sf.hub_film_hash_key 
			WHERE 
				f.hub_film_hash_key IS NULL AND 
				sf.load_end_date IS NULL
		)
		UPDATE edw.sat_film_static AS sf 
		SET 
			load_end_date = ds.load_end_date
		FROM 
			deleted_sat ds
		WHERE 
			sf.hub_film_hash_key = ds.hub_film_hash_key AND 
			sf.load_date = ds.load_date;
	END;
$$ LANGUAGE plpgsql;		

-- Процедура для заполнения саттелита "изменяемая информация по фильмам"
CREATE OR REPLACE PROCEDURE edw.load_sat_film_dynamic(current_update_dt timestamp)
AS $$
	BEGIN 
		-- Добавляем новые строки по фильмам
		INSERT INTO edw.sat_film_dynamic (hub_film_hash_key, load_date, load_end_date, record_source, hash_diff, rental_duration, rental_rate, replacement_cost)
		SELECT 
			f.hub_film_hash_key,
			f.load_date,
			NULL                     AS load_end_date,
			f.record_source,
			f.film_dynamic_hash_diff,
			f.rental_duration, 
			f.rental_rate, 
			f.replacement_cost
		FROM 
			staging.film f
			LEFT JOIN edw.sat_film_dynamic sf ON sf.hub_film_hash_key = f.hub_film_hash_key AND sf.load_end_date IS NULL 
		WHERE 
			sf.hub_film_hash_key IS NULL OR 
			sf.hash_diff <> f.film_dynamic_hash_diff;
		
		-- Помечаем неактуальные строки
		WITH updated_sat AS (
			SELECT 
				sf1.hub_film_hash_key,
				sf1.load_date,
				sf2.load_date          AS load_end_date 
			FROM 
				edw.sat_film_dynamic sf1
				JOIN edw.sat_film_dynamic sf2 ON sf2.hub_film_hash_key = sf1.hub_film_hash_key 
					AND sf2.load_date > sf1.load_date 
					AND sf1.load_end_date IS NULL 
					AND sf2.load_end_date IS NULL 
		)
		UPDATE edw.sat_film_dynamic AS sf 
		SET 
			load_end_date = us.load_end_date
		FROM 
			updated_sat us
		WHERE 
			sf.hub_film_hash_key = us.hub_film_hash_key AND 
			sf.load_date = us.load_date;
		
		-- Помечаем удаленные строки
		WITH deleted_sat AS (
			SELECT 
				sf.hub_film_hash_key,
				sf.load_date,
				current_update_dt     AS load_end_date 
			FROM 
				edw.sat_film_dynamic sf
				LEFT JOIN staging.film f ON f.hub_film_hash_key = sf.hub_film_hash_key 
			WHERE 
				f.hub_film_hash_key IS NULL AND 
				sf.load_end_date IS NULL
		)
		UPDATE edw.sat_film_dynamic AS sf 
		SET 
			load_end_date = ds.load_end_date
		FROM 
			deleted_sat ds
		WHERE 
			sf.hub_film_hash_key = ds.hub_film_hash_key AND 
			sf.load_date = ds.load_date;
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
-- Процедура по созданию таблиц Mart-слоя
CREATE OR REPLACE PROCEDURE mart.create_all_tables()
AS $$
	BEGIN 
		DROP TABLE IF EXISTS mart.rented_film;
	
		-- Таблица с количеством аренд дисков по фильмам		
		CREATE TABLE mart.rented_film (
			film_title varchar(100) NOT NULL,
			quantity   int4 NOT NULL		
		);
	END; 
$$ LANGUAGE plpgsql;
--==================================================================================--

--=============================== ЗАПОЛНЕНИЕ ДАННЫМИ ===============================--
-- Процедура для заполнения таблицы rented_film
CREATE OR REPLACE PROCEDURE mart.rented_film()
AS $$
	BEGIN 
		TRUNCATE TABLE mart.rented_film;
		INSERT INTO mart.rented_film (film_title, quantity)
		SELECT 
			sfs.title           AS film_title,
			count(hr.rental_id) AS quantity
		FROM edw.hub_film hf
			JOIN edw.sat_film_static sfs ON sfs.hub_film_hash_key = hf.hub_film_hash_key
			JOIN edw.link_film_inventory lfi ON lfi.hub_film_hash_key = hf.hub_film_hash_key
			JOIN edw.hub_inventory hi ON hi.hub_inventory_hash_key = lfi.hub_inventory_hash_key
			JOIN edw.link_rental_inventory lri ON lri.hub_inventory_hash_key = hi.hub_inventory_hash_key
			JOIN edw.hub_rental hr ON hr.hub_rental_hash_key = lri.hub_rental_hash_key
		GROUP BY 1
		ORDER BY 2 DESC;
	END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--============================ ФИНАЛЬНЫЕ ПРОЦЕДУРЫ =================================--
-- Пересоздание всех таблиц
CREATE OR REPLACE PROCEDURE reset_full_data()
AS $$
	BEGIN 
		CALL staging.create_all_tables();
		CALL edw.create_all_tables();
		CALL mart.create_all_tables();
	END;
$$ LANGUAGE plpgsql;

-- Загрузка данных в хранилище
CREATE OR REPLACE PROCEDURE load_full_data()
AS $$
	DECLARE 
		var_current_update_dt timestamp = now();
	BEGIN 
		CALL staging.upload_film(var_current_update_dt);
		CALL staging.upload_inventory(var_current_update_dt);
		CALL staging.upload_rental(var_current_update_dt);
	
		CALL edw.load_hub_film();
		CALL edw.load_hub_inventory();
		CALL edw.load_hub_rental();
	
		CALL edw.load_link_film_inventory();
		CALL edw.load_link_rental_inventory();
	
		CALL edw.load_sat_film_static(var_current_update_dt);
		CALL edw.load_sat_film_dynamic(var_current_update_dt);
	
		CALL mart.rented_film();
	END;
$$ LANGUAGE plpgsql;
--==================================================================================--

--============================ ВЫЗОВ ФИНАЛЬНЫХ ПРОЦЕДУР ============================--
-- !Выполнять только когда требуется полная перезапись хранилища!
CALL reset_full_data();
--------- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ------------

-- Финальная процедура загрузки данных
CALL load_full_data();
--==================================================================================--











