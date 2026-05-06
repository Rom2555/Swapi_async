import asyncio
import aiohttp
import aiosqlite

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
    except Exception:
        pass
    return None


async def main():
    # Инициализация БД
    await init_db()

    async with aiohttp.ClientSession() as session:
        # Получаем общее число персонажей
        async with session.get("https://www.swapi.tech/api/people/") as r:
            total = (await r.json())["total_records"]

        async with aiosqlite.connect(DB) as db:
            # Выгружаем всех персонажей (ID от 1 до total)
            for start in range(1, total + 1, 50):
                end = min(start + 49, total)
                print(f"Загрузка {start}-{end} из {total}...")

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
                print(f"  сохранено {len(chars)} записей")

    # Итог
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM characters")
        row = await cursor.fetchone()
        print(f"\nВсего в базе: {row[0]} персонажей")


if __name__ == "__main__":
    asyncio.run(main())
