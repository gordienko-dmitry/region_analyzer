from collections import defaultdict
from aiohttp import web
import numpy as np
import logging

from api.classes import Citizen, ValidateError
from api.orm import middleware as sql


HTTP_CODES_ERRORS = {
    400: '400: Bad Request',
    404: '404: Not Found',
    500: '500: Internal Server Error'
}


def answer(func):
    async def wrapper(*args, **kwargs):
        code, data = await func(*args, **kwargs)
        if code in HTTP_CODES_ERRORS:
            return web.Response(status=code, text=HTTP_CODES_ERRORS[code])
        else:
            return web.json_response({'data': data}, status=code)
    return wrapper


async def check_import_id(import_id):
    """
    Checking import_id for correct
    """
    import_id_base = await sql.get_import_id()
    if import_id > import_id_base:
        logging.error('import_id = {}, but in base only {}'.format(import_id, import_id_base))
        raise ValidateError('import_id too big')


def read_int_vars_from_url(request, vars):
    """
    Read vars from url
    """
    list_vars = []
    for name in vars:
        list_vars.append(int(request.match_info[name]))
    return list_vars


def check_citizen(one_citizen, field_number=None):
    """
    Check citizen: fields number, validations of each field
    """
    citizen = Citizen()
    if field_number and field_number != len(one_citizen):
        logging.error('Wrong number of fields for citizen_id = {}'.format(one_citizen.get('citizen_id')))
        raise ValidateError('Wrong number of fields')
    for key, value in one_citizen.items():
        setattr(citizen, key, value)
    return citizen


def check_citizens(citizens_json, check_relatives):
    """
    Check citizens, each citizen and relatives of all import
    """
    citizens = []
    field_number = len(Citizen.__slots__)
    right_relative = defaultdict(int)
    for one_citizen in citizens_json:
        citizen = check_citizen(one_citizen, field_number)
        citizens.append(citizen)

        if check_relatives and citizen.relatives:
            citizen_id = citizen.citizen_id
            relaive_of_citizen = defaultdict(int)
            for relative_id in citizen.relatives:
                relaive_of_citizen[relative_id] += 1
                if citizen_id > relative_id:
                    right_relative['{}{}'.format(relative_id, citizen_id)] -= 1
                elif citizen_id < relative_id:
                    right_relative['{}{}'.format(citizen_id, relative_id)] += 1

            for _, relative_count in relaive_of_citizen.items():
                if relative_count > 1:
                    logging.error('Wrong relative count for id = {} '.format(citizen_id))
                    raise ValidateError('Wrong relatives')

    for ids, value in right_relative.items():
        if value != 0:
            logging.error('Wrong relatives = {}'.format(ids))
            raise ValidateError('Wrong relatives')
    return citizens


def get_percentile(towns_ages):
    """
    Getting percentile for list of ages
    """
    list_for_returns = []
    for town_age in towns_ages:
        p50, p75, p99 = np.percentile(town_age['ages'], [50,75,99])
        list_for_returns.append({'town':town_age['town'],
                                 'p50': round(p50, 2),
                                 'p75': round(p75, 2),
                                 'p99': round(p99,2)})
    return list_for_returns


async def import_citizens_prepare(request):
    """
    Preparing for citizens import operation
    """
    citizens_json = await request.json()
    citizens_json = citizens_json['citizens']
    return [check_citizens(citizens_json, True)]


async def update_citizens_prepare(request):
    """
    Preparing for updating operation
    """
    list_of_data = read_int_vars_from_url(request, ['import_id', 'citizen_id'])
    await check_import_id(list_of_data[0])
    citizen_json = await request.json()
    list_of_data.append(check_citizen(citizen_json))

    relaive_of_citizen = defaultdict(int)
    if citizen_json.get('relatives'):
        for relative_id in list_of_data[2].relatives:
            relaive_of_citizen[relative_id] += 1

        for _, relative_count in relaive_of_citizen.items():
            if relative_count > 1:
                logging.error('Wrong relative count for id = {}'.format(list_of_data[1]))
                raise ValidateError('Wrong relatives')

    return list_of_data


async def get_import_id_prepare(request):
    """
    Preparing for get citizens operation
    """
    list_of_data = read_int_vars_from_url(request, ['import_id'])
    await check_import_id(list_of_data[0])
    return list_of_data


async def import_citizens_sql(citizens):
    """
    Run sql script for adding citizens
    """
    import_id = await sql.import_citizens(citizens)
    return 201, {'import_id': import_id}


async def update_citizens_sql(import_id, citizen_id, citizen):
    """
    Run sql script for updating citizen
    """
    citizen_json = await sql.update_citizen(import_id, citizen_id, citizen)
    if citizen_json:
        return 200, citizen_json
    else:
        return 400, ''


async def get_citizens_sql(import_id):
    """
    Run sql script for getting citizens for import_id
    """
    citizens_json = await sql.get_citizens(import_id)
    return 200, citizens_json


async def get_presents_sql(import_id):
    """
    Run sql script for getting presents months
    """
    presents_json = await sql.get_presents(import_id)
    return 200, presents_json


async def get_percentile_sql(import_id):
    """
    Run sql script for getting data for percentile func
    """
    towns_ages = await sql.get_ages(import_id)
    list_percentile = get_percentile(towns_ages)
    return 200, list_percentile


@answer
async def start_process(request, method):
    """
    Main procedure of logic - preparing input data, run sql scripts
    """
    prepare_methods = {
        'import': import_citizens_prepare,
        'update': update_citizens_prepare,
        'get': get_import_id_prepare,
        'get_presents': get_import_id_prepare,
        'get_percentile': get_import_id_prepare,
    }

    sql_methods = {
        'import': import_citizens_sql,
        'update': update_citizens_sql,
        'get': get_citizens_sql,
        'get_presents': get_presents_sql,
        'get_percentile': get_percentile_sql,
    }

    try:
        parametres = await prepare_methods[method](request)
    except ValidateError as error:
        logging.exception('Validate error in prepare for method {}'.format(method))
        return 400, ''
    except Exception as error:
        logging.exception('Unexpected error (while preparing) in method {}'.format(method))
        return 400, ''

    try:
        return await sql_methods[method](*parametres)
    except ValidateError as error:
        logging.exception('Validate error in sql method {}'.format(method))
        return 400, ''
    except Exception as error:
        logging.exception('Unexpected error (SQL) in method {}'.format(method))
        return 500, ''

