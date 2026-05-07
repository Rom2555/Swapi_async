import asyncio
import aiohttp
import aiosqlite
import logging
import os
import json
from dotenv import load_dotenv

# Глобальный кэш для названий, чтобы не делать одинаковые запросы
CACHE = {}
CACHE_LOCK = asyncio.Lock()

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB = os.getenv("DB", "swapi_characters.db")
MIGRATION = os.getenv("MIGRATION", "migration.sql")
URL_TEMPLATE = os.getenv("URL", "https://www.swapi.tech/api/people/{id}/")
API_PEOPLE_URL = os.getenv("API_PEOPLE_URL", "https://www.swapi.tech/api/people/")


async def fetch_json(session, url, max_retries=None):
    """Универсальная функция получения JSON с проверкой статуса и повторными попытками."""
    if max_retries is None:
        max_retries = int(os.getenv("RETRY_MAX", 3))

    last_error = None

    for attempt in range(max_retries):
        try:
            async with session.get(url) as r:
                # 1. Сначала проверяем статус
                if r.status != 200:
                    if r.status == 404:
                        logger.warning(f"Ресурс {url} не найден (404)")
                        return None
                    logger.warning(
                        f"HTTP {r.status} для {url} (попытка {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt) # Задержка
                        continue
                    return None

                # Попытка распарсить JSON
                try:
                    return await r.json()
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as e:
                    logger.warning(
                        f"Ошибка декодирования JSON для {url}: {e} (попытка {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None

        except asyncio.TimeoutError:
            logger.warning(f"Таймаут для {url} (попытка {attempt + 1}/{max_retries})")
            last_error = "Timeout"
        except aiohttp.ClientError as e:
            logger.warning(f"Сетевая ошибка для {url}: {e} (попытка {attempt + 1}/{max_retries})")
            last_error = str(e)
        except Exception as e:
            logger.exception(f"Неожиданная ошибка для {url}: {e}")
            return None

        # Задержка перед следующей попыткой при сетевых ошибках
        if attempt < max_retries - 1:
            await asyncio.sleep(2 ** attempt)

    logger.error(f"Не удалось получить данные после {max_retries} попыток для {url}. Последняя ошибка: {last_error}")
    return None


async def fetch_resource_name(session, url):
    """Вспомогательная функция: получает имя из URL с использованием кэша и блокировки."""
    if not url:
        return None

    # Быстрая проверка без блокировки (если уже есть - сразу отдаем)
    if url in CACHE:
        return CACHE[url]

    # Если нет - занимаем "замок", чтобы другие задачи не лезли в сеть одновременно
    async with CACHE_LOCK:
        # Вторая проверка: пока мы ждали замок, другая задача могла уже скачать данные.
        # Поэтому проверяем кэш еще раз внутри блокировки.
        if url in CACHE:
            return CACHE[url]

        # Если данных всё еще нет — скачиваем (только одна задача будет здесь в данный момент)
        logger.debug(f"Downloading URL (cache miss): {url}")
        data = await fetch_json(session, url)

        name = None
        if data and data.get("message") == "ok":
            props = data.get("result", {}).get("properties", {})
            name = props.get("title") or props.get("name")

        # Сохраняем в кэш
        if name:
            CACHE[url] = name

        return name


async def fetch(session, char_id):
    """Возвращает данные персонажа или None."""
    data = await fetch_json(session, URL_TEMPLATE.format(id=char_id))
    if not data or data.get("message") != "ok":
        return None

    p = data["result"]["properties"]

    # Подготовка всех URL для параллельного запроса
    urls_to_fetch = []

    homeworld_url = p.get("homeworld")
    if homeworld_url:
        urls_to_fetch.append(homeworld_url)

    # Добавляем URL из списков (films, species, starships, vehicles)
    list_fields = {
        "films": p.get("films", []),
        "species": p.get("species", []),
        "starships": p.get("starships", []),
        "vehicles": p.get("vehicles", [])
    }

    for urls in list_fields.values():
        urls_to_fetch.extend(urls)

    # Параллельный запрос всех названий (планеты, фильмов и т.д.)
    results = await asyncio.gather(*[fetch_resource_name(session, url) for url in urls_to_fetch])

    # Разбор результатов
    # Результаты приходят в том же порядке, что и urls_to_fetch
    result_iter = iter(results)

    # Получаем планету
    homeworld_name = next(result_iter, None)
    if not homeworld_name:
        logger.warning(f"Не удалось получить название планеты для персонажа {char_id}")

    # Получаем списки названий
    def get_names_for_list(urls):
        count = len(urls)
        names = []
        for _ in range(count):
            name = next(result_iter, None)
            if name:
                names.append(name)
        return ",".join(names) if names else None

    films_str = get_names_for_list(list_fields["films"])
    species_str = get_names_for_list(list_fields["species"])
    starships_str = get_names_for_list(list_fields["starships"])
    vehicles_str = get_names_for_list(list_fields["vehicles"])

    # Формируем кортеж для вставки
    record = (
        char_id,
        p.get("birth_year"),
        p.get("eye_color"),
        p.get("gender"),
        p.get("hair_color"),
        homeworld_name,
        p.get("mass"),
        p.get("name"),
        p.get("skin_color"),
        films_str,
        species_str,
        starships_str,
        vehicles_str,
    )

    # Логирование значений ПЕРЕД вставкой
    logger.debug(
        f"Подготовлена запись для ID {char_id}: Name={p.get('name')}, Homeworld={homeworld_name}, Films count={len(films_str.split(',')) if films_str else 0}")

    return record


async def init_db():
    """Создает таблицу через SQL-миграцию с обработкой ошибок."""
    try:
        async with aiosqlite.connect(DB) as db:
            # Попытка выполнить миграцию из файла
            migration_success = False
            if os.path.exists(MIGRATION):
                try:
                    with open(MIGRATION, encoding="utf-8") as f:
                        migration_sql = f.read()

                    if "CREATE TABLE" in migration_sql and "IF NOT EXISTS" not in migration_sql:
                        logger.warning("В файле миграции отсутствует IF NOT EXISTS, рекомендуется добавить.")

                    await db.executescript(migration_sql)
                    await db.commit()
                    logger.info(f"Миграция из файла {MIGRATION} выполнена успешно")
                    migration_success = True
                except (FileNotFoundError, IOError) as e:
                    logger.warning(f"Не удалось прочитать файл миграции {MIGRATION}: {e}")
                except aiosqlite.Error as e:
                    logger.error(f"Ошибка выполнения миграции из файла: {e}")

            if not migration_success:
                logger.info("Попытка создать таблицу базовым скриптом...")
                await db.execute("""
                                 CREATE TABLE IF NOT EXISTS characters
                                 (
                                     id         INTEGER PRIMARY KEY,
                                     birth_year TEXT,
                                     eye_color  TEXT,
                                     gender     TEXT,
                                     hair_color TEXT,
                                     homeworld  TEXT,
                                     mass       TEXT,
                                     name       TEXT,
                                     skin_color TEXT,
                                     films      TEXT,
                                     species    TEXT,
                                     starships  TEXT,
                                     vehicles   TEXT
                                 )
                                 """)
                await db.commit()
                logger.info("Базовая структура таблицы создана/проверена")

    except aiosqlite.Error as e:
        logger.error(f"Критическая ошибка подключения к БД {DB}: {e}")
        raise


async def main():
    # Инициализация БД
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Не удалось инициализировать БД: {e}")
        return

    connector_limit = int(os.getenv("CONNECTOR_LIMIT", 10))
    timeout_total = int(os.getenv("TIMEOUT", 30))
    batch_size = int(os.getenv("BATCH_SIZE", 10))

    connector = None
    session = None

    try:
        connector = aiohttp.TCPConnector(limit=connector_limit)
        timeout = aiohttp.ClientTimeout(total=timeout_total)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        # Получаем общее число персонажей
        total_data = await fetch_json(session, API_PEOPLE_URL)
        if not total_data:
            logger.error("Не удалось получить данные от API (возвращено None)")
            return
        if "total_records" not in total_data:
            logger.error(f"Некорректный ответ API: {total_data}")
            return
        total = total_data["total_records"]
        logger.info(f"Общее число персонажей: {total}")

        total_saved = 0

        # Используем отдельное соединение для записи
        async with aiosqlite.connect(DB) as db:
            for start in range(1, total + 1, batch_size):
                end = min(start + batch_size - 1, total)
                logger.info(f"Обработка батча {start}-{end} из {total}...")

                tasks = [fetch(session, i) for i in range(start, end + 1)]
                # gather возвращает результаты в том же порядке, что и tasks
                results = await asyncio.gather(*tasks)

                # Фильтруем None (неудачные запросы)
                chars = [c for c in results if c]

                if chars:
                    try:
                        await db.executemany(
                            """
                            REPLACE INTO characters
                            (id, birth_year, eye_color, gender, hair_color,
                             homeworld, mass, name, skin_color,
                             films, species, starships, vehicles)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            chars,
                        )
                        await db.commit()
                        total_saved += len(chars)
                        logger.info(f"  Сохранено {len(chars)} записей (всего: {total_saved})")
                    except aiosqlite.Error as e:
                        logger.error(f"Ошибка при вставке данных в БД: {e}")
                        await db.rollback()
                else:
                    logger.warning(f"  Нет валидных данных для диапазона {start}-{end}")

        # Проверка полноты данных после завершения
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM characters")
            row = await cursor.fetchone()
            actual_count = row[0]

            print(f"\n--- ИТОГИ ---")
            print(f"Ожидалось персонажей: {total}")
            print(f"Фактически сохранено: {actual_count}")

            if actual_count < total:
                diff = total - actual_count
                logger.warning(
                    f"Несоответствие: не сохранилось {diff} персонажей. Проверьте логи на предмет ошибок API.")
            else:
                logger.info("Загрузка завершена успешно, все данные на месте.")

    except Exception as e:
        logger.exception(f"Критическая ошибка в процессе загрузки: {e}")
    finally:
        # Гарантированное закрытие сессии
        if session and not session.closed:
            await session.close()
            logger.info("HTTP сессия закрыта")
        if connector:
            await connector.close()


if __name__ == "__main__":
    asyncio.run(main())
