import pytest

from api.classes import Citizen, ValidateError
import api.orm.middleware as sql
import api.logic as logic


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
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": ["1"] }],

    # extra field
    [ { "citizen_id": 1, "town": "Москва", "street": "Льва Толстого", "building": "16к7стр5", "apartment": 7,
        "name": "Иванов Иван Иванович", "birth_date": "26.12.1986", "gender": "male", "relatives": [],
        "skills": "party maker" }]
    ]


# TEST CLASS CITIZENS

FOR_CITIZENS = [
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


class TestClasses:

    @pytest.fixture(scope="function", params=
        FOR_CITIZENS
    )
    def params_citizens(self, request):
        return request.param

    def test_make_citizen(self, params_citizens):
        (citizen_json, result) = params_citizens
        citizen = Citizen()
        if not result:
            with pytest.raises(ValidateError):
                for key, value in citizen_json.items():
                    setattr(citizen, key, value)
        else:
            for key, value in citizen_json.items():
                setattr(citizen, key, value)


class TestLogicWithoutDB:

    @pytest.mark.asyncio
    async def test_check_import_id(self):
        import_id = await sql.get_import_id()
        await logic.check_import_id(import_id)
        with pytest.raises(ValidateError):
            await logic.check_import_id(import_id + 1)

    @pytest.fixture(scope="function", params=
        ERROR_PACKAGES
                    )
    def params_citizens(self, request):
        return request.param

    def test_check_citizen(self, params_citizens):
        with pytest.raises(ValidateError):
            logic.check_citizen(params_citizens[0], len(Citizen.__slots__))

    def test_check_citizens(self, params_citizens):
        with pytest.raises(ValidateError):
            logic.check_citizens(params_citizens, True)

    def test_get_percentile(self):
        test_package = [{'town': 'Керчь','ages': [40]},
                        {'town': 'Москва', 'ages': [30, 20, 30, 90, 90, 50, 50]},
                        {'town': 'Питер', 'ages': [20, 20]}]

        result = [{'town': 'Керчь', 'p50': 40.0, 'p75': 40.0, 'p99': 40.0},
                  {'town': 'Москва', 'p50': 50.0, 'p75': 70.0, 'p99': 90.0},
                  {'town': 'Питер', 'p50': 20.0, 'p75': 20.0, 'p99': 20.0}]

        percentile_data = logic.get_percentile(test_package)

        for ind, town_data in enumerate(percentile_data):
            assert result[ind]['town'] == town_data['town']
            assert result[ind]['p50'] == town_data['p50']
            assert result[ind]['p75'] == town_data['p75']
            assert result[ind]['p99'] == town_data['p99']
