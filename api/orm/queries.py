CREATE_CITIZENS = '''
CREATE TABLE citizens(
    id serial PRIMARY KEY,
    citizen_id integer,
    import_id integer,
    town text,
    street text,
    building text,
    apartment integer,
    name text,
    birth_date date,
    gender text,
    age integer
)
'''

CREATE_RELATIVES = '''
CREATE TABLE relatives(
    id serial PRIMARY KEY,
    citizen_id integer,
    import_id integer,
    relative_id integer,
    birth_month integer
)
'''

CREATE_CONSTS = '''
CREATE TABLE consts(
    key text PRIMARY KEY,
    value integer,
    value_text text
)
'''

CREATE_INDEX_CITIZENS = '''
CREATE INDEX ON citizens (import_id, citizen_id)
'''

CREATE_INDEX_RELATIVES = '''
CREATE INDEX ON relatives (import_id, citizen_id, relative_id)
'''

CITIZEN_COLUMNS = 'citizen_id,import_id,town,street,building,apartment,name,birth_date,gender,age'
RELATIVE_COLUMNS = 'citizen_id,import_id,relative_id,birth_month'


VAR = '${}'

INSERT_IMPORT_ID_0 = '''
insert into consts (key, value) values ('import_id', 0)
'''

UPDATE_IMPORT_ID = '''
update consts set value = {} where key='import_id'
'''

UPDATE_CITIZEN = '''
update citizens set {} where import_id={} and citizen_id={}
'''


SELECT_IMPORT_ID = '''
select consts.value from consts as consts where key='import_id'
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

SELECT_CITIZEN = '''
select citizens.citizen_id, citizens.town, citizens.street, citizens.building, citizens.apartment, citizens.name, 
       citizens.birth_date, citizens.gender, json_agg(relative_id) as relatives 
       from citizens left join relatives on 
          citizens.citizen_id = relatives.citizen_id and citizens.import_id = relatives.import_id 
        where citizens.import_id={} and citizens.citizen_id={}
group by citizens.citizen_id, citizens.town, citizens.street, citizens.building, citizens.apartment, citizens.name, 
        citizens.birth_date, citizens.gender
'''

SELECT_RELATIVES = '''
select relative_id from relatives where import_id={} and citizen_id={}
'''

SELECT_RELATIVES_BIRTH_MONTH = '''
select citizen_id, birth_date from citizens where import_id={} and citizen_id in ({})
'''

SELECT_PRESENTS = '''
select citizen_id, count(relative_id), birth_month from relatives where import_id = {} group by citizen_id, birth_month 
    order by birth_month, citizen_id
'''

SELECT_AGES = '''
select town, json_agg(age) as ages from citizens where import_id = {} group by town order by town
'''

DELETE_RELATIVES = '''
delete from relatives where import_id={} and citizen_id in ({}) and relative_id in ({})
'''