from datetime import datetime
import subprocess
import aiohttp
import asyncio
import asyncpg
import pytest
import json
import time

import api.orm.sql as sql


'''
TEST DATA
'''

BASE_PACKAGE = [
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [2]
      },
    { "citizen_id": 2, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Сергей Иванович", "birth_date": "01.04.1997", "gender": "male", "relatives": [1]
      },
    { "citizen_id": 3, "town": "Керчь", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.11.1986", "gender": "female", "relatives": []
      }]


# PACKAGES

ERROR_PACKAGES = [

    # Citizen_id
    [{"citizen_id": "1", "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": []}],

    [{"citizen_id": 0, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": []}],

    # town
    [ { "citizen_id": 1, "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": 14, "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    # street
    [ { "citizen_id": 1, "town": "Москва", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": 47, "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    # building
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "apartment": 7, "name": "Иванов Иван Иванович",
        "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": 16, "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    # apartment
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5",
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 0,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": "11к4",
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    # name
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "", "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": 1010011010, "birth_date": "26.12.1986", "gender": "male", "relatives": [] }],

    # birth_date
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "31.02.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "2612.1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26/12/1986", "gender": "male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": 18, "gender": "male", "relatives": [] }],

    # gender
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "relatives": [2] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "co-male", "relatives": [] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": 42, "relatives": [] }],

    # relatives
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male"}],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [2] }],

    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": ["1"] }],

    # extra field
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [],
        "skills": "party maker" }]
    ]


# PACKAGE FOR UPDATE

UPDATE_PACKAGE = [
    # EMPTY
    ({"town": ""}, False),
    ({"street": ""}, False),
    ({"building": ""}, False),
    ({"apartment": 0}, False),
    ({"name": ""}, False),
    ({"birth_date": ""}, False),
    ({"gender": ""}, False),

    # TYPES
    ({"town": 1}, False),
    ({"street": 3}, False),
    ({"building": 3}, False),
    ({"apartment": ""}, False),
    ({"name": 14}, False),
    ({"birth_date": "28122019"}, False),
    ({"birth_date": 28}, False),
    ({"gender": 14}, False),
    ({"relatives": ""}, False),
    ({"relatives": [""]}, False),

    # Correct
    # 1
    ({"town": "Самара"}, True),
    ({"street": "Карандышева"}, True),
    ({"building": "9к9"}, True),
    ({"apartment": 15}, True),
    ({"name": "Николаев Семен"}, True),
    ({"birth_date": "01.01.1987"}, True),
    ({"gender": "female"}, True),
    ({"relatives": [1, 3]}, True),

    # > 1
    ({"town": "Ростов", "street": "Советская", "building": "74", "apartment": 11,
     "name": "Леон", "birth_date": "14.09.1994", "gender": "male", "relatives": []}, True),

    ]


# PRESENTS
PRESENT_PACKAGE = [
    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.01.1989", "gender": "male", "relatives": [] },
    ], {}),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.01.1989", "gender": "male", "relatives": [1] },
    ], {'1':  [{"citizen_id": 1, "presents": 1}],}),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.02.1989", "gender": "male", "relatives": [1] },
     ], {'2': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.03.1989", "gender": "male", "relatives": [1] },
     ], {'3': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.04.1989", "gender": "male", "relatives": [1] },
     ], {'4': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.05.1989", "gender": "male", "relatives": [1] },
     ], {'5': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.06.1989", "gender": "male", "relatives": [1] },
     ], {'6': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.07.1989", "gender": "male", "relatives": [1] },
     ], {'7': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.08.1989", "gender": "male", "relatives": [1] },
     ], {'8': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.09.1989", "gender": "male", "relatives": [1] },
     ], {'9': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.10.1989", "gender": "male", "relatives": [1] },
     ], {'10': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.11.1989", "gender": "male", "relatives": [1] },
     ], {'11': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1989", "gender": "male", "relatives": [1] },
     ], {'12': [{"citizen_id": 1, "presents": 1}], }),

    ([
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1989", "gender": "male", "relatives": [2,3,4,5,6,7,8,9,10] },
    { "citizen_id": 2, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Сергей Иванович", "birth_date": "01.04.1999", "gender": "male", "relatives": [1]},
    { "citizen_id": 3, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1989", "gender": "male", "relatives": [1] },
    { "citizen_id": 4, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.12.1929", "gender": "male", "relatives": [1] },
    { "citizen_id": 5, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Сергей Иванович", "birth_date": "01.04.1929", "gender": "male", "relatives": [1]},
    { "citizen_id": 6, "town": "Москва", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.04.1969", "gender": "female", "relatives": [1]},
    { "citizen_id": 7, "town": "Москва", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.04.1969", "gender": "female", "relatives": [1]},
    { "citizen_id": 8, "town": "Керчь", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.04.1979", "gender": "female", "relatives": [1]},
    { "citizen_id": 9, "town": "Питер", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.04.1999", "gender": "female", "relatives": [1]},
    { "citizen_id": 10, "town": "Питер", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.04.1999", "gender": "female", "relatives": [1]},

     ], {'4': [{"citizen_id": 1, "presents": 7}],
         '12': [{"citizen_id": 1, "presents": 2}, {"citizen_id": 2, "presents": 1}, {"citizen_id": 3, "presents": 1},
              {"citizen_id": 4, "presents": 1}, {"citizen_id": 5, "presents": 1}, {"citizen_id": 6, "presents": 1},
              {"citizen_id": 7, "presents": 1}, {"citizen_id": 8, "presents": 1}, {"citizen_id": 9, "presents": 1},
              {"citizen_id": 10, "presents": 1}] })
]


# PERCENTILE
PERCENTILE_PACKAGE = [
    { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.02.1989", "gender": "male", "relatives": [2] },
    { "citizen_id": 2, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Сергей Иванович", "birth_date": "01.04.1999", "gender": "male", "relatives": [1]},
    { "citizen_id": 3, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.02.1989", "gender": "male", "relatives": [] },
    { "citizen_id": 4, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Иван Иванович", "birth_date": "26.02.1929", "gender": "male", "relatives": [] },
    { "citizen_id": 5, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
      "name": "Иванов Сергей Иванович", "birth_date": "01.04.1929", "gender": "male", "relatives": []},
    { "citizen_id": 6, "town": "Москва", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.01.1969", "gender": "female", "relatives": []},
    { "citizen_id": 7, "town": "Москва", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.01.1969", "gender": "female", "relatives": []},

    { "citizen_id": 8, "town": "Керчь", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.01.1979", "gender": "female", "relatives": []},

    { "citizen_id": 9, "town": "Питер", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.01.1999", "gender": "female", "relatives": []},

    { "citizen_id": 10, "town": "Питер", "street": "Иосифа Бродского", "building": "2", "apartment": 11,
      "name": "Романова Мария Леонидовна", "birth_date": "23.01.1999", "gender": "female", "relatives": []},
]

'''
QUERY DATA
'''

SELECT_CITIZENS = '''
select citizens.citizen_id, citizens.town, citizens.street, citizens.building, citizens.apartment, citizens.name, 
       citizens.birth_date, citizens.gender, json_agg(relative_id) as relatives 
       from citizens left join relatives on 
          citizens.citizen_id = relatives.citizen_id and citizens.import_id = relatives.import_id 
        where citizens.import_id={} 
group by citizens.citizen_id, citizens.town, citizens.street, citizens.building, citizens.apartment, citizens.name, 
        citizens.birth_date, citizens.gender
'''

class TestAPI:

    url = 'http://localhost:9090/{}'
    def setup_class(cls):
        """
        Start server for tests
        """
        cls.p = subprocess.Popen(['python3', '/home/region_analyzer/runner.py', '-p', '9090'])
        time.sleep(3)

    def teardown_class(cls):
        """
        End for tests
        """
        try:
            cls.p.kill()
        except:
            pass

    @pytest.fixture(scope="function", params=
        ERROR_PACKAGES
    )
    def params_imports(self, request):
        """
        Parameters for error imports
        """
        return request.param

    @pytest.fixture(scope="function", params=
        UPDATE_PACKAGE
    )
    def params_update(self, request):
        """
        Parameters for update
        """
        return request.param

    @pytest.mark.asyncio
    async def test_help(self):
        """
        Test for help message
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url.format('')) as resp:
                assert resp.status == 200

    @pytest.mark.asyncio
    async def test_import_errors(self, params_imports):
        """
        Test imports with errors
        """
        package = {'citizens': params_imports}
        url = self.url.format('imports')
        # Errors
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bytes(json.dumps(package), 'utf-8')) as resp:
                assert resp.status == 400

    @pytest.mark.asyncio
    async def test_import_correct(self):
        """
        Test correct imports
        """
        package = {'citizens': BASE_PACKAGE}
        url = self.url.format('imports')
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bytes(json.dumps(package), 'utf-8')) as resp:
                assert resp.status == 201
                import_id = json.loads(await resp.text())['data']['import_id']
        connection = await sql.connect()
        records = await sql.fetch_query(connection, SELECT_CITIZENS.format(import_id))
        for ind, record in enumerate(records):
            citizen = package['citizens'][ind]
            for key, value in citizen.items():
                if key == 'relatives':
                    v = record['relatives']
                    if v == '[null]':
                        list_relative = []
                    else:
                        list_relative = json.loads(v)
                    assert list_relative == value
                elif key == 'birth_date':
                    assert datetime.strptime(value, '%d.%m.%Y').date() == record[key]
                else:
                    assert value == record[key]
        await sql.close(connection)

    @pytest.mark.asyncio
    async def test_update(self, params_update):
        """
        Tests for updating citizens
        """
        (package, result) = params_update
        package_import = {'citizens': BASE_PACKAGE}
        url_import = self.url.format('imports')
        async with aiohttp.ClientSession() as session:
            async with session.post(url_import, data=bytes(json.dumps(package_import), 'utf-8')) as resp:
                import_id = json.loads(await resp.text())['data']['import_id']
            url = self.url.format('imports/{}/citizens/{}'.format(import_id, 1))
            async with session.patch(url, data=bytes(json.dumps(package), 'utf-8')) as resp:
                if result:
                    assert resp.status == 200
                    citizen = json.loads(await resp.text())['data']
                else:
                    assert resp.status == 400
                    return

        for key, value in package.items():
            assert value == citizen[key]


    @pytest.mark.asyncio
    async def test_get(self):
        """
        Test get method
        """
        package_import = {'citizens': BASE_PACKAGE}
        url_import = self.url.format('imports')
        async with aiohttp.ClientSession() as session:
            async with session.post(url_import, data=bytes(json.dumps(package_import), 'utf-8')) as resp:
                import_id = json.loads(await resp.text())['data']['import_id']
            url = self.url.format('imports/{}/citizens'.format(import_id))
            async with session.get(url) as resp:
                assert resp.status == 200
                result = json.loads(await resp.text())['data']

        for ind, citizen in enumerate(BASE_PACKAGE):
            for key, value in citizen.items():
                assert value == result[ind][key]

    @pytest.fixture(scope="function", params=
        PRESENT_PACKAGE
    )
    def params_presents(self, request):
        return request.param

    @pytest.mark.asyncio
    async def test_presents(self, params_presents):
        """
        Test for presents
        """
        (package, result) = params_presents
        package = {'citizens': package}
        url_import = self.url.format('imports')
        async with aiohttp.ClientSession() as session:
            async with session.post(url_import, data=bytes(json.dumps(package), 'utf-8')) as resp:
                import_id = json.loads(await resp.text())['data']['import_id']
            url = self.url.format('imports/{}/citizens/birthdays'.format(import_id))
            async with session.get(url) as resp:
                assert resp.status == 200
                birthdays_data = json.loads(await resp.text())['data']

        for month, month_data in birthdays_data.items():
            if result.get(month) is None:
                assert month_data == []
            else:
                assert month_data == result[month]

    @pytest.fixture(scope="function", params=[
        (PERCENTILE_PACKAGE, [{'town': 'Керчь', 'p50': 40.0, 'p75': 40.0, 'p99': 40.0},
                              {'town': 'Москва', 'p50': 50.0, 'p75': 70.0, 'p99': 90.0},
                              {'town': 'Питер', 'p50': 20.0, 'p75': 20.0, 'p99': 20.0}])
    ])
    def params_percentile(self, request):
        return request.param

    @pytest.mark.asyncio
    async def test_percentile(self, params_percentile):
        """
        Test fot percentile
        """
        (package, result) = params_percentile
        package = {'citizens': package}
        url_import = self.url.format('imports')
        async with aiohttp.ClientSession() as session:
            async with session.post(url_import, data=bytes(json.dumps(package), 'utf-8')) as resp:
                import_id = json.loads(await resp.text())['data']['import_id']
            url = self.url.format('imports/{}/towns/stat/percentile/age'.format(import_id))
            async with session.get(url) as resp:
                assert resp.status == 200
                percentile_data = json.loads(await resp.text())['data']

        for ind, town_data in enumerate(percentile_data):
            assert result[ind]['town'] == town_data['town']
            assert result[ind]['p50'] == town_data['p50']
            assert result[ind]['p75'] == town_data['p75']
            assert result[ind]['p99'] == town_data['p99']
