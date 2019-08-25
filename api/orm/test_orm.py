from asyncpg.protocol.protocol import _create_record as Record
from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import collections
import asyncio
import asyncpg
import pytest

import api.orm.middleware as middleware
import api.orm.migrate as migrate
from api.classes import Citizen
import api.orm.sql as sql


class TestSQL:

    def setup_class(self):
        self.database = 'testdb'
        asyncio.get_event_loop().run_until_complete(self.create_testdb())

    @staticmethod
    async def create_testdb():
        connection = await asyncpg.connect(user=sql.USER, password=sql.PASSWORD,
                                        host=sql.HOST, port=sql.PORT, timeout=10)

        await sql.execute_query(connection, 'CREATE DATABASE testdb')
        await sql.close(connection)

    def teardown_class(self):
        asyncio.get_event_loop().run_until_complete(self.drop_testdb())

    @staticmethod
    async def drop_testdb():
        connection = await asyncpg.connect(user=sql.USER, password=sql.PASSWORD,
                                        host=sql.HOST, port=sql.PORT, timeout=10)

        await sql.execute_query(connection, 'DROP DATABASE testdb')
        await sql.close(connection)

    @pytest.mark.asyncio
    async def test_sql(self):
        sql.DATABASE = self.database
        connection = await sql.connect()

        await sql.execute_query(connection, 'CREATE TABLE testdict( id serial PRIMARY KEY, value integer, key text)')
        await sql.copy_records_to_table(connection, 'testdict', [('big', 500), ('small', 200)], ['key', 'value'])

        record = await sql.fetch_query(connection, 'select value from testdict where key=\'big\'', True)
        assert record['value'] == 500
        record = await sql.fetch_query(connection, 'select value from testdict where key=\'middle\'', True)
        assert record == None
        records = await sql.fetch_query(connection, 'select key, value from testdict order by value')
        assert len(records) == 2
        assert_list = [200, 500]
        k = 0
        for record in records:
            assert record['value'] == assert_list[k]
            k += 1

        await sql.execute_query(connection, 'DROP TABLE testdict')
        with pytest.raises(sql.SQLExecuteError):
            await sql.copy_records_to_table(connection, 'testdict', [('big', 500), ('small', 200)], ['key', 'value'])

        await sql.close(connection)


class TestMiddleware:

    def setup_class(self):
        sql.DATABASE = 'testdb'
        asyncio.get_event_loop().run_until_complete(self.create_testdb())

    @staticmethod
    async def create_testdb():
        connection = await asyncpg.connect(user=sql.USER, password=sql.PASSWORD,
                                        host=sql.HOST, port=sql.PORT, timeout=10)

        await sql.execute_query(connection, 'CREATE DATABASE testdb')
        await migrate.create_tables()
        await sql.close(connection)

    def teardown_class(self):
        asyncio.get_event_loop().run_until_complete(self.drop_testdb())

    @staticmethod
    async def drop_testdb():
        connection = await asyncpg.connect(user=sql.USER, password=sql.PASSWORD,
                                        host=sql.HOST, port=sql.PORT, timeout=10)

        await sql.execute_query(connection, 'DROP DATABASE testdb')
        await sql.close(connection)

    @pytest.mark.asyncio
    async def test_get_import_id(self):
        connection = await sql.connect()
        await sql.execute_query(connection, 'update consts set value = 11 where key=\'import_id\'')
        await sql.close(connection)

        import_id = await middleware.get_import_id()
        assert import_id == 11

    def get_citizens(self):
        citizen1 = Citizen()
        citizen1.citizen_id = 1
        citizen1.town = 'Татуин'
        citizen1.street = 'Ленина'
        citizen1.building = '1'
        citizen1.apartment = 1
        citizen1.name = 'Энакин'
        citizen1.birth_date = '25.05.1977'
        citizen1.gender = 'male'
        citizen1.relatives = [2]

        citizen2 = Citizen()
        citizen2.citizen_id = 2
        citizen2.town = 'Набу'
        citizen2.street = 'Карла-Маркса'
        citizen2.building = '14'
        citizen2.apartment = 1
        citizen2.name = 'Падме'
        citizen2.birth_date = '25.06.1977'
        citizen2.gender = 'female'
        citizen2.relatives = [1]

        return [citizen1, citizen2]

    @pytest.mark.asyncio
    async def test_import_citizens(self):
        citizens = self.get_citizens()
        import_id = await middleware.import_citizens(citizens)

        connection = await sql.connect()
        records = await sql.fetch_query(connection, 'select * from citizens where import_id={} order by citizen_id'.
                                  format(import_id))

        for ind in range(2):
            assert records[ind]['citizen_id'] == citizens[ind].citizen_id
            assert records[ind]['town'] == citizens[ind].town
            assert records[ind]['street'] == citizens[ind].street
            assert records[ind]['building'] == citizens[ind].building
            assert records[ind]['apartment'] == citizens[ind].apartment
            assert records[ind]['name'] == citizens[ind].name
            assert records[ind]['birth_date'].strftime('%m/%d/%Y') == citizens[ind].birth_date.strftime('%m/%d/%Y')
            assert records[ind]['gender'] == citizens[ind].gender
            assert records[ind]['age'] == datetime.today().year - citizens[ind].birth_date.year

        records = await sql.fetch_query(connection, 'select * from relatives where import_id={} order by citizen_id'.
                                  format(import_id))

        assert records[0]['relative_id'] == 2
        assert records[1]['relative_id'] == 1

        assert records[0]['birth_month'] == 6
        assert records[1]['birth_month'] == 5

        await sql.close(connection)

    @pytest.mark.asyncio
    async def test_get_update_import_id(self):
        connection = await sql.connect()
        import_id1 = await middleware.get_update_import_id(connection)
        import_id2 = await middleware.get_update_import_id(connection)
        await sql.close(connection)
        assert import_id1 + 1 == import_id2

    def test_prepare_tables(self):
        today = datetime.today()
        citizens = self.get_citizens()
        table_citizens, table_relatives = middleware.prepare_tables(citizens, 4)

        assert len(table_relatives) == 2
        assert len(table_citizens) == 2

        assert table_citizens[0] == (1, 4, 'Татуин', 'Ленина', '1',
                              1, 'Энакин', datetime(1977, 5, 25, 0, 0), 'male',
                                     relativedelta(today, datetime(1977, 5, 25, 0, 0)).years)
        assert table_citizens[1] == (2, 4, 'Набу', 'Карла-Маркса', '14',
                              1, 'Падме', datetime(1977, 6, 25, 0, 0), 'female',
                                      relativedelta(today, datetime(1977, 6, 25, 0, 0)).years)

        assert table_relatives[0] == (2, 4, 1, 5)
        assert table_relatives[1] == (1, 4, 2, 6)


    def test_make_citizen_dict(self):
        columns = collections.OrderedDict([('citizen_id', 0), ('town', 1), ('street', 2), ('building', 3),
                                           ('apartment', 4), ('name', 5), ('birth_date', 6), ('gender', 7),
                                           ('relatives', 8)])

        record_json = {'citizen_id' : 1, 'town': 'Татуин', 'street': 'Ленина', 'building': '1', 'apartment': 1,
                       'name': 'Энакин', 'birth_date': '25.05.1977', 'gender': 'male', 'relatives': []}

        record_tuple = (1, 'Татуин', 'Ленина', '1',  1, 'Энакин', date(1977, 5, 25), 'male', '[null]')
        record = Record(columns, record_tuple)
        citizen_json = middleware.make_citizen_dict(record)
        for key, value in citizen_json.items():
            assert value == record_json[key]

        record_json = {'citizen_id' : 2, 'town': 'Набу', 'street': 'Карла-Маркса', 'building': '14', 'apartment': 1,
                       'name': 'Падме', 'birth_date': '25.06.1977', 'gender': 'female', 'relatives': [1, 3]}

        record_tuple = (2, 'Набу', 'Карла-Маркса', '14',  1, 'Падме', date(1977, 6, 25), 'female', '[1, 3]')
        record = Record(columns, record_tuple)
        citizen_json = middleware.make_citizen_dict(record)
        for key, value in citizen_json.items():
            assert value == record_json[key]

    @pytest.mark.asyncio
    async def test_update_citizen(self):
        citizens = self.get_citizens()
        import_id = await middleware.import_citizens(citizens)

        citizen = Citizen()
        citizen.name = 'Дарт'
        citizen.street = 'Пшш'
        await middleware.update_citizen(import_id, 1, citizen)
        citizens[0].name = 'Дарт'
        citizens[0].street = 'Пшш'

        connection = await sql.connect()
        records = await sql.fetch_query(connection, 'select * from citizens where import_id={} order by citizen_id'.
                                  format(import_id))

        for ind in range(2):
            assert records[ind]['citizen_id'] == citizens[ind].citizen_id
            assert records[ind]['town'] == citizens[ind].town
            assert records[ind]['street'] == citizens[ind].street
            assert records[ind]['building'] == citizens[ind].building
            assert records[ind]['apartment'] == citizens[ind].apartment
            assert records[ind]['name'] == citizens[ind].name
            assert records[ind]['birth_date'].strftime('%m/%d/%Y') == citizens[ind].birth_date.strftime('%m/%d/%Y')
            assert records[ind]['gender'] == citizens[ind].gender
            assert records[ind]['age'] == datetime.today().year - citizens[ind].birth_date.year

        records = await sql.fetch_query(connection, 'select * from relatives where import_id={} order by citizen_id'.
                                  format(import_id))

        assert records[0]['relative_id'] == 2
        assert records[1]['relative_id'] == 1

        assert records[0]['birth_month'] == 6
        assert records[1]['birth_month'] == 5

        await sql.close(connection)


    @pytest.mark.asyncio
    async def test_get_citizens(self):
        citizens = self.get_citizens()
        import_id = await middleware.import_citizens(citizens)

        records = await middleware.get_citizens(import_id)

        for ind in range(2):
            assert records[ind]['citizen_id'] == citizens[ind].citizen_id
            assert records[ind]['town'] == citizens[ind].town
            assert records[ind]['street'] == citizens[ind].street
            assert records[ind]['building'] == citizens[ind].building
            assert records[ind]['apartment'] == citizens[ind].apartment
            assert records[ind]['name'] == citizens[ind].name
            assert records[ind]['birth_date'] == citizens[ind].birth_date.strftime('%d.%m.%Y')
            assert records[ind]['gender'] == citizens[ind].gender
            assert records[ind]['relatives'] == citizens[ind].relatives

    @pytest.mark.asyncio
    async def test_get_presents(self):
        connection = await sql.connect()
        import_id = await middleware.get_update_import_id(connection)
        await sql.execute_query(connection, 'insert into relatives ("citizen_id", "relative_id", "birth_month", "import_id")'
                                      ' values (1,2,2,{0}), (1,3,2,{0}), (2,1,2,{0}), (3,1,5,{0})'.format(import_id))
        await sql.close(connection)
        presents = await middleware.get_presents(import_id)
        for month in range(1,13):
            if month == 2:
                assert presents[month][0]['citizen_id'] == 1
                assert presents[month][0]['presents'] == 2
                assert presents[month][1]['citizen_id'] == 2
                assert presents[month][1]['presents'] == 1
                assert len(presents[month]) == 2
            elif month == 5:
                assert presents[month][0]['citizen_id'] == 3
                assert presents[month][0]['presents'] == 1
                assert len(presents[month]) == 1
            else:
                assert presents[month] == []

    @pytest.mark.asyncio
    async def test_get_ages(self):
        citizens = self.get_citizens()
        import_id = await middleware.import_citizens(citizens)

        list_ages = await middleware.get_ages(import_id)
        assert list_ages[0]['town'] == 'Набу'
        assert list_ages[0]['ages'] == [42]
        assert list_ages[1]['town'] == 'Татуин'
        assert list_ages[0]['ages'] == [42]

