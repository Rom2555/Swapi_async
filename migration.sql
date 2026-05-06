-- Скрипт миграции базы данных для таблицы персонажей Star Wars
-- Создает таблицу characters с необходимыми полями

CREATE TABLE IF NOT EXISTS characters (
    id INTEGER PRIMARY KEY,
    birth_year TEXT,
    eye_color TEXT,
    gender TEXT,
    hair_color TEXT,
    homeworld TEXT,
    mass TEXT,
    name TEXT NOT NULL,
    skin_color TEXT
);

-- Очищаем таблицу перед загрузкой новых данных
DELETE FROM characters;