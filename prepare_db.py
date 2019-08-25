import asyncio

from api.orm.migrate import create_tables


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(create_tables())
