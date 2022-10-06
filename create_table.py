import asyncio
from more_itertools import chunked
import aiohttp
from sqlalchemy import  Integer, String, Column, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config


CHUNK_SIZE = 2
engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    name = Column(String(128),nullable = False)
    birth_year = Column(String(128),nullable = False)
    eye_color = Column(String(128),nullable = False)
    films = Column(JSON, nullable=False)
    gender = Column(String(128),nullable = False)
    hair_color = Column(String(128),nullable = False)
    height = Column(String(128),nullable = False)
    homeworld = Column(String(128),nullable = False)
    mass = Column(String(80),nullable = False)
    skin_color = Column(String(50),nullable = False)
    species = Column(JSON, nullable=False)
    starships = Column(JSON, nullable=False)
    vehicles = Column(JSON, nullable=False)


async def get_people(session, people_id):
        result = await session.get(f'https://swapi.dev/api/people/{people_id}')
        return await result.json()


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with aiohttp.ClientSession() as web_session:
        for chunk_id in chunked(range(1,17), CHUNK_SIZE):
            coros =[get_people(web_session, i) for i in chunk_id]
            result = await asyncio.gather(*coros)
            people_list = [People(id=item.get("id", None),
                                  name=item.get("name", None),
                                  birth_year=item.get("birth_year", None),
                                  eye_color=item.get("eye_color", None),
                                  films=item.get("films", None),
                                  gender=item.get("gender", None),
                                  hair_color=item.get("hair_color", None),
                                  height=item.get("height", None),
                                  homeworld=item.get("homeworld", None),
                                  mass=item.get("mass", None),
                                  skin_color=item.get("skin_color", None),
                                  species=item.get("species", None),
                                  starships=item.get("starships", None),
                                  vehicles=item.get("vehicles", None)) for item in result]
            async with async_session_maker() as orm_session:
                orm_session.add_all(people_list)
                await orm_session.commit()


if __name__ == '__main__':
    asyncio.run(main())