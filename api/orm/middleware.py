from dateutil.relativedelta import relativedelta
from datetime import datetime
import logging
import json

import api.orm.queries as queries
import api.orm.sql as sql
from api import classes


async def get_import_id():
    """
    Get import_id from DB
    """
    connection = await sql.connect()
    result = await sql.fetch_query(connection, queries.SELECT_IMPORT_ID, row=True)
    await sql.close(connection)
    return result['value']


async def import_citizens(citizens_list):
    """
    Import list of citizens into DB
    """
    citizen_columns, relative_columns = get_columns()

    connection = await sql.connect()
    async with connection.transaction():
        import_id = await get_update_import_id(connection)

        citizen_table, relative_table = prepare_tables(citizens_list, import_id)

        await sql.copy_records_to_table(connection, 'citizens', records=citizen_table, columns=citizen_columns)
        await sql.copy_records_to_table(connection, 'relatives', records=relative_table, columns=relative_columns)

    await sql.close(connection)
    return import_id


async def get_update_import_id(connection):
    """
    Getting and updating import_id
    """
    result = await sql.fetch_query(connection, queries.SELECT_IMPORT_ID, row=True)
    import_id = result['value'] + 1
    await sql.execute_query(connection, queries.UPDATE_IMPORT_ID.format(import_id))
    return import_id


def prepare_tables(citizens_list, import_id):
    """
    Preparing tables for mass import
    """
    citizen_table = []
    relative_table = []
    today = datetime.today()
    for citizen in citizens_list:
        citizen_table.append((citizen.citizen_id, import_id, citizen.town, citizen.street, citizen.building,
                              citizen.apartment, citizen.name, citizen.birth_date, citizen.gender,
                              relativedelta(today, citizen.birth_date).years))
        if citizen.relatives:
            for relative in citizen.relatives:
                relative_table.append((relative, import_id, citizen.citizen_id, citizen.birth_date.month))

    return citizen_table, relative_table


def get_columns():
    """
    Getting columns names like lists for mass import
    """
    return queries.CITIZEN_COLUMNS.split(','), queries.RELATIVE_COLUMNS.split(',')


def make_citizen_dict(citizen_record):
    """
    Make dict from record object
    """
    dict_json = {}
    for field in classes.Citizen.__slots__:
        if field == 'input':
            continue
        if field == 'relatives':
            array = citizen_record[field]
            dict_json[field] = json.loads('[]' if array == '[null]' else array)
        elif field == 'birth_date':
            dict_json[field] = citizen_record[field].strftime('%d.%m.%Y')
        else:
            dict_json[field] = citizen_record[field]
    return dict_json


async def update_citizen(import_id, citizen_id, citizen):
    """
    Updating citizen in DB
    """
    text_set = ''
    number_var = 1
    args = []
    relatives = None
    for arg in citizen.__slots__:
        try:
            value = getattr(citizen, arg)
        except AttributeError:
            continue
        if arg in ('citizen_id', 'relatives') or value is None:
            if arg == 'relatives':
                relatives = value
            continue
        text_set += ', {}=${}'.format(arg, number_var)
        args.append(value)
        number_var += 1

    text_query = queries.UPDATE_CITIZEN.format(text_set[2:], import_id, citizen_id)
    text_select_citizen = queries.SELECT_CITIZEN.format(import_id, citizen_id)
    connection = await sql.connect()
    async with connection.transaction():
        if text_set:
            await sql.execute_query(connection, text_query, *args)
        if relatives is not None:
            text_query_select_relatives = queries.SELECT_RELATIVES
            records = await sql.fetch_query(connection, text_query_select_relatives.format(import_id, citizen_id))

            for_delete = ''
            for record in records:
                if record['relative_id'] in relatives:
                    relatives.remove(record['relative_id'])
                else:
                    for_delete += ',{}'.format(record['relative_id'])

            if for_delete:
                text_query_delete_relatives = queries.DELETE_RELATIVES
                await sql.execute_query(connection, text_query_delete_relatives.format(import_id, citizen_id,
                                                                                     for_delete[1:]))
                await sql.execute_query(connection, text_query_delete_relatives.format(import_id, for_delete[1:],
                                                                                  citizen_id))

            if relatives:
                list_sql = ','.join(str(e) for e in relatives)
                if citizen_id not in relatives:
                    list_sql += ',' + citizen_id
                text_query_relatives_month = queries.SELECT_RELATIVES_BIRTH_MONTH
                records = await sql.fetch_query(connection, text_query_relatives_month.format(import_id, list_sql))
                if len(records) != len(relatives):
                    logging.error('Wrong relatives')
                    raise classes.ValidateError
                birthdays = {}
                for record in records:
                    birthdays[record['citizen_id']] = record['birth_date'].month

                relative_columns = queries.RELATIVE_COLUMNS.split(',')
                table_relatives = []
                for relative in relatives:
                    table_relatives.append((citizen_id, import_id, relative, birthdays[relative]))
                    if relative != citizen_id:
                        table_relatives.append((relative, import_id, citizen_id, birthdays[citizen_id]))

                await sql.copy_records_to_table(connection, 'relatives', records=table_relatives,
                                                columns=relative_columns)


        citizen_record = await sql.fetch_query(connection, text_select_citizen, True)
        if citizen_record is None:
            dict_json = {}
        else:
            dict_json = make_citizen_dict(citizen_record)

    await sql.close(connection)
    return dict_json


async def get_citizens(import_id):
    """
    Get citizens from DB
    """
    text_query_citizens = queries.SELECT_CITIZENS

    list_citizens = []
    connection = await sql.connect()
    async with connection.transaction():
        citizens_records = await sql.fetch_query(connection, text_query_citizens.format(import_id))
        for citizen_record in citizens_records:
            dict_json = make_citizen_dict(citizen_record)
            list_citizens.append(dict_json)

    await sql.close(connection)
    return list_citizens

async def get_presents(import_id):
    """
    Get data for percentile solving
    """
    text_query_presents = queries.SELECT_PRESENTS.format(import_id)

    connection = await sql.connect()
    present_records = await sql.fetch_query(connection, text_query_presents)

    dict_presents_month = {x: [] for x in range(1,13)}
    for record in present_records:
        dict_presents_month[record['birth_month']].append({'citizen_id': record['citizen_id'],
                                                           'presents': record['count']})

    await sql.close(connection)
    return dict_presents_month


async def get_ages(import_id):
    """
    Get towns and ages for presents solving
    """
    text_query_ages = queries.SELECT_AGES.format(import_id)

    list_of_dicts = []
    connection = await sql.connect()
    records_town = await sql.fetch_query(connection, text_query_ages)
    for record in records_town:
        list_of_dicts.append({'town': record['town'],
                              'ages': json.loads('[]' if record['ages'] == '[null]' else record['ages'])})

    await sql.close(connection)
    return list_of_dicts

