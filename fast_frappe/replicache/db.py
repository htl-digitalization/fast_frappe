# import asyncio
import asyncpg


default_space_id = 'test'


async def pg_init():
    return await asyncpg.connect("postgresql://pgvectortest:123@pgvector:5432/pgvectortest")


async def tx(func):
    conn = await pg_init()
    async with conn.transaction(isolation="serializable"):
        result = await conn.execute(func)
    return result


# # Example usage
# async def main():
#     async def example_query(conn):
#         result = await conn.fetch("SELECT * FROM your_table")
#         print(result)

#     await tx(example_query)

# asyncio.run(main())
