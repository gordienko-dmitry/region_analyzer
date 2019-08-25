HELP = '''
Welcome to region analyzer API.

Allowed next queries:

1.  POST /imports
Post information about citizens

body:
{"citizens": 
    [{"citizen_id": [int],"town": {str], "street": [str], "building": [str], "apartment": [int], "name": [str], 
    "birth_date": [str], "gender": ["male", ["female"], "relatives": [list]}, ...]
}


2. PATCH /imports/$import_id/citizens/$citizen_id
Update information of citizen

body (any of this fields):
{"town": {str], "street": [str], "building": [str], "apartment": [int], "name": [str], 
    "birth_date": [str], "gender": ["male", ["female"], "relatives": [list]}


3. GET /imports/$import_id/citizens
Get citizens info with current import_id


4.  GET /imports/$import_id/citizens/birthdays
Get presents calendar for current import_id  


5. GET /imports/$import_id/towns/stat/percentile/age
Get percentile age for different towns  

'''