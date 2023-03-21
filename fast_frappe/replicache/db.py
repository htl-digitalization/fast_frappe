# import asyncio
import asyncpg


default_space_id = 'test'


def pg_init():
    return asyncpg.connect("postgresql://pgvectortest:123@pgvector:5432/pgvectortest")


conn = pg_init()


async def tx(sql):
    async with conn.transaction(isolation="serializable"):
        # Postgres requires non-scrollable cursors to be created
        # and used in a transaction.
        try:
            await conn.execute(sql)
        except Exception as e:
            print(e)


async def cursor(sql, fetch_number=1):
    async with conn.transaction(isolation="serializable"):
        # Postgres requires non-scrollable cursors to be created
        # and used in a transaction.
        try:
            cursor = await conn.cursor(sql)
            rows = await cursor.fetch(fetch_number)
            if fetch_number == 1:
                return [dict(row) for row in rows][0]
            else:
                return [dict(row) for row in rows]
        except Exception as e:
            print(e)
            return None
