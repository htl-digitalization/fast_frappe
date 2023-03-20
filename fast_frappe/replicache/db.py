# import asyncio
import asyncpg


default_space_id = 'test'


async def pg_init():
    return await asyncpg.connect("postgresql://pgvectortest:123@pgvector:5432/pgvectortest")


async def tx(sql):
    conn = await pg_init()
    async with conn.transaction(isolation="serializable"):
        # Postgres requires non-scrollable cursors to be created
        # and used in a transaction.
        try:
            cursor = await conn.cursor(sql)
            # await cursor.scroll(0, mode='absolute')
            # await cursor.fetch()
            # records = []
            # async for row in cursor:
            #     records.append(dict(row))
            rows = await cursor.fetch(1)
            data = [dict(row) for row in rows][0]
            # await cursor.close()
            return data
        except:
            return None


# # Example usage
# async def main():
#     async def example_query(conn):
#         result = await conn.fetch("SELECT * FROM your_table")
#         print(result)

#     await tx(example_query)

# asyncio.run(main())
