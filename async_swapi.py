import asyncio
import aiohttp
import aiosqlite
import logging
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB = os.getenv("DB", "swapi_characters.db")  # Файл базы данных
MIGRATION = os.getenv("MIGRATION", "migration.sql")
URL = os.getenv("URL", "https://www.swapi.tech/api/people/{id}/")


async def fetch_json(session, url, max_retries=3):
    """Универсальная функция получения JSON с проверкой статуса и повторными попытками."""
    for attempt in range(max_retries):
        try:
            async with session.get(url) as r:
                # Проверка статуса ответа до разбора JSON
                if r.status != 200:
                    logger.warning(
                        f"HTTP {r.status} для {url} (попытка {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                        continue
                    return None
                return await r.json()
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут для {url} (попытка {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"Сетевая ошибка для {url}: {e} (попытка {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            return None
        except Exception as e:
            logger.exception(f"Неожиданная ошибка для {url}: {e}")
            return None
    return None


async def fetch(session, char_id):
    """Возвращает данные персонажа или None."""
    data = await fetch_json(session, URL.format(id=char_id))
    if not data or data.get("message") != "ok":
        if data and data.get("message") != "ok":
            logger.warning(
                f"API вернул не-ok ответ для персонажа {char_id}: {data.get('message', 'Неизвестная ошибка')}"
            )
        return None

    p = data["result"]["properties"]

    # Получаем название планеты вместо URL
    homeworld_name = None
    homeworld_url = p.get("homeworld")
    if homeworld_url:
        planet_data = await fetch_json(session, homeworld_url)
        if planet_data and planet_data.get("message") == "ok":
            homeworld_name = planet_data["result"]["properties"].get("name")
        else:
            logger.warning(f"Не удалось получить данные планеты для персонажа {char_id}")

    # Получаем названия для списков (films, species, starships, vehicles)
    # Извлекаем названия/ID из URL
    def extract_names(urls):
        """Извлекает названия из списка URL."""
        if not urls:
            return None
        names = []
        for url in urls:
            if url:
                # Из URL вида https://www.swapi.tech/api/films/1/ извлекаем последний не пустой элемент
                parts = [p for p in url.strip().rstrip('/').split('/') if p]
                if parts:
                    names.append(parts[-1])
        return ",".join(names) if names else None

    films_str = extract_names(p.get("films", []))
    species_str = extract_names(p.get("species", []))
    starships_str = extract_names(p.get("starships", []))
    vehicles_str = extract_names(p.get("vehicles", []))

    return (
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


async def init_db():
    """Создает таблицу через SQL-миграцию с обработкой ошибок."""
    try:
        async with aiosqlite.connect(DB) as db:
            # Сначала выполняем основной скрипт миграции
            try:
                with open(MIGRATION, encoding="utf-8") as f:
                    migration_sql = f.read()
                await db.executescript(migration_sql)
                await db.commit()
                logger.info("Миграция выполнена успешно")
            except FileNotFoundError:
                logger.error(f"Файл миграции не найден: {MIGRATION}")
                raise
            except aiosqlite.Error as e:
                logger.error(f"Ошибка выполнения миграции: {e}")
                await db.rollback()
                raise
    except aiosqlite.Error as e:
        logger.error(f"Ошибка подключения к БД {DB}: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при инициализации БД: {e}")
        raise


async def main():
    # Инициализация БД
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Не удалось инициализировать БД: {e}")
        return

    # Настройка сессии с лимитом подключений и таймаутом
    connector_limit = int(os.getenv("CONNECTOR_LIMIT", 10))
    timeout_total = int(os.getenv("TIMEOUT", 30))
    batch_size = int(os.getenv("BATCH_SIZE", 50))
    connector = None
    session = None

    try:
        connector = aiohttp.TCPConnector(limit=connector_limit)
        timeout = aiohttp.ClientTimeout(total=timeout_total)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        # Получаем общее число персонажей
        people_url = os.getenv("API_PEOPLE_URL", "https://www.swapi.tech/api/people/")
        total_data = await fetch_json(session, people_url)
        if not total_data:
            logger.error("Не удалось получить данные от API (возвращено None)")
            return
        if "total_records" not in total_data:
            logger.error(f"Некорректный ответ API: {total_data}")
            return
        total = total_data["total_records"]
        logger.info(f"Общее число персонажей: {total}")

        async with aiosqlite.connect(DB) as db:
            # Выгружаем всех персонажей (ID от 1 до total)
            total_saved = 0
            for start in range(1, total + 1, batch_size):
                end = min(start + batch_size - 1, total)
                logger.info(f"Загрузка {start}-{end} из {total}...")

                tasks = [fetch(session, i) for i in range(start, end + 1)]
                chars = [c for c in await asyncio.gather(*tasks) if c]

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
                        logger.info(f"  сохранено {len(chars)} записей (всего: {total_saved})")
                    except aiosqlite.Error as e:
                        logger.error(f"Ошибка при вставке данных в БД: {e}")
                        await db.rollback()
                else:
                    logger.warning(f"  нет данных для диапазона {start}-{end}")

        # Итог - проверка полноты данных
        async with aiosqlite.connect(DB) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM characters")
            row = await cursor.fetchone()
            actual_count = row[0]
            print(f"\nВсего в базе: {actual_count} персонажей")
            print(f"Ожидалось: {total}, Фактически сохранено: {actual_count}")
            if actual_count < total:
                logger.warning(
                    f"Несоответствие: ожидалось {total}, сохранено {actual_count} "
                    f"(разница: {total - actual_count})"
                )
            else:
                logger.info("Все данные успешно загружены")

    except Exception as e:
        logger.exception(f"Ошибка в процессе загрузки: {e}")
    finally:
        # Корректное закрытие сессии
        if session and not session.closed:
            await session.close()
            logger.info("Сессия закрыта")
        if connector:
            await connector.close()


if __name__ == "__main__":
    asyncio.run(main())
