import asyncio
import aiohttp
import aiosqlite
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB = "swapi_characters.db"
MIGRATION = "migration.sql"
URL = "https://www.swapi.tech/api/people/{id}/"


async def init_db():
    """Создает таблицу через SQL-миграцию."""
    async with aiosqlite.connect(DB) as db:
        await db.executescript(open(MIGRATION, encoding="utf-8").read())
        await db.commit()


async def fetch(session, char_id):
    """Возвращает данные персонажа или None."""
    try:
        async with session.get(URL.format(id=char_id)) as r:
            data = await r.json()
            if data.get("message") == "ok":
                p = data["result"]["properties"]
                return (
                    char_id,
                    p.get("birth_year"),
                    p.get("eye_color"),
                    p.get("gender"),
                    p.get("hair_color"),
                    p.get("homeworld"),
                    p.get("mass"),
                    p.get("name"),
                    p.get("skin_color"),
                )
            else:
                logger.warning(f"API вернул не-ok ответ для персонажа {char_id}: {data.get('message', 'Неизвестная ошибка')}")
                return None
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при получении персонажа {char_id}")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка клиента при получении персонажа {char_id}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при получении персонажа {char_id}: {e}")
        return None


async def main():
    # Инициализация БД
    await init_db()

    # Настройка сессии с лимитом подключений и таймаутом
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Получаем общее число персонажей
        async with session.get("https://www.swapi.tech/api/people/") as r:
            total = (await r.json())["total_records"]

        async with aiosqlite.connect(DB) as db:
            # Выгружаем всех персонажей (ID от 1 до total)
            for start in range(1, total + 1, 50):
                end = min(start + 49, total)
                logger.info(f"Загрузка {start}-{end} из {total}...")

                tasks = [fetch(session, i) for i in range(start, end + 1)]
                chars = [c for c in await asyncio.gather(*tasks) if c]

                await db.executemany(
                    """
                    INSERT INTO characters
                    (id, birth_year, eye_color, gender, hair_color,
                     homeworld, mass, name, skin_color)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    chars,
                )
                await db.commit()
                logger.info(f"  сохранено {len(chars)} записей")

    # Итог
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM characters")
        row = await cursor.fetchone()
        print(f"\nВсего в базе: {row[0]} персонажей")


if __name__ == "__main__":
    asyncio.run(main())
