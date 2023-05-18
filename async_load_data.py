import asyncio
import aiohttp
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople

async def get_quantity():
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/')
    json_data = await response.json()
    await session.close()
    return json_data

async def get_people(person_id):
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{person_id}')
    json_data = await response.json()
    await session.close()
    return json_data

async def load_to_db(people_json):
    async with Session() as session:
        orm_objects = [SwapiPeople(json=item) for item in people_json]
        session.add_all(orm_objects)
        await session.commit()

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    quantity_json = await get_quantity()
    quantity = quantity_json['count']

    people_coros = (get_people(i) for i in range(1,quantity))
    people_coros_chunked = chunked(people_coros, 5)

    for people_coros_chunk in people_coros_chunked:
        people = await asyncio.gather(*people_coros_chunk)
        asyncio.create_task(load_to_db(people))
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks)

if __name__ == '__main':
    asyncio.run(main())
