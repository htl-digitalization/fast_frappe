import asyncpg

default_space_id = 'test'


async def pg_init():
    return await asyncpg.connect("postgresql://pgvectortest:123@pgvector:5432/pgvectortest")


async def tx(conn, sql):
    async with conn.transaction(isolation="serializable"):
        try:
            await conn.execute(sql)
        except Exception as e:
            print(e)


async def cursor(conn, sql, fetch_number=1):
    async with conn.transaction(isolation="serializable"):
        # async with conn.cursor(sql) as cursor:
        print(sql)
        rows = await conn.fetch(sql)
        if len(rows) == 0:
            return None
        elif fetch_number == 1:
            return [dict(row) for row in rows][0]
        else:
            return [dict(row) for row in rows]
