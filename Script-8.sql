SET search_path TO public;

-- 0. Установка расширения pg_cron (на всякий случай)
CREATE EXTENSION IF NOT EXISTS pg_cron;


-- 1. Создание таблиц
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users_audit;
CREATE TABLE IF NOT EXISTS users (
	id SERIAL PRIMARY KEY,
	name TEXT,
	email TEXT,
	user_role TEXT, -- стоит принудительный UPPERCASE ключевых слов, поэтому записано не ROLE, а user_role
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- время последнего изменения записи
);
CREATE TABLE IF NOT EXISTS users_audit (
	id SERIAL PRIMARY KEY,
	user_id INTEGER,       -- чьего пользователя была изменена запись (не делаем FOREIGN KEY, чтобы удалить пользователя было проще)
	changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	changed_by TEXT,
	field_changed TEXT,
	old_value TEXT,
	new_value TEXT
);


-- 2. Функция-триггер для аудита изменений
CREATE OR REPLACE FUNCTION log_users_update()
RETURNS TRIGGER AS $$
BEGIN
	-- Обновляем поле updated_at на текущее время при любом обновлении:
	NEW.updated_at := NOW();

	/* 1) Если изменилось имя */
	IF OLD.name IS DISTINCT FROM NEW.name THEN  -- Выражение OLD.field IS DISTINCT FROM NEW.field возвращает TRUE, если значения отличаются (учитывая NULL корректно).
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (
			OLD.id,
			current_user,     -- кто сделал изменение (имя текущего SQL-пользователя)
			'name',
			OLD.name,
			NEW.name
		);
	END IF;
	
	-- 2) Если изменился email
	IF OLD.email IS DISTINCT FROM NEW.email THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (
			OLD.id,
			current_user,
			'email',
			OLD.email,
			NEW.email
		);
	END IF;
	
	-- 3) Если изменился user_role
	IF OLD.user_role IS DISTINCT FROM NEW.user_role THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (
			OLD.id,
			current_user,
			'user_role',
			OLD.user_role,
			NEW.user_role
		);
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 3. Создание триггера на users (логирование изменений)
DROP TRIGGER IF EXISTS trigger_log_user_changes ON users;
CREATE TRIGGER trigger_log_user_changes
BEFORE UPDATE ON users  -- перед сохранением каждой изменяемой строки таблицы users будет вызываться триггерная функция.
FOR EACH ROW
EXECUTE FUNCTION log_users_update();


-- 4. Функция экспорта вчерашних изменений в CSV
CREATE OR REPLACE FUNCTION export_users_audit_csv()
RETURNS TEXT -- будем возвращать текстовый статус, например 'OK' или сообщение об ошибке
AS $$
DECLARE
	export_date TEXT := to_char(CURRENT_DATE - 1, 'YYYY_MM_DD');
	filename TEXT := '/tmp/users_audit_export_' || export_date || '.csv';
	sql_ TEXT;
BEGIN
	-- Строим SQL-строку для COPY с нужным условием и файлом
	sql_ :=
		'COPY (SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at ' ||
		'FROM users_audit ' ||
		'WHERE changed_at::date = ' || quote_literal(CURRENT_DATE - 1) ||
		') TO ' || quote_literal(filename) || ' WITH CSV HEADER';

-- Выполняем команду COPY, сконструированную динамически
	EXECUTE sql_;

	RAISE NOTICE 'Экспорт файл создан: %', filename;
	RETURN 'OK: ' || export_date;
EXCEPTION
	WHEN OTHERS THEN
		RAISE warning 'Ошибка экспорта: %', SQLERRM;
		RETURN 'ERROR';
END;
$$ LANGUAGE plpgsql;


-- 5. Планирование ежедневного запуска в 03:00 ночи
SELECT cron.schedule(
	'daily_users_audit_export',            -- имя задачи
	'0 3 * * *',                           -- crontab: каждую ночь в 3:00
	$$ SELECT export_users_audit_csv(); $$ -- команда выполнения функции
);
	