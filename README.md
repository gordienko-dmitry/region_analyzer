# REGION ANALYZER
*Test excersize from Yandex Backend School*
  
This is rest API for analyze citizens data.  
We have strong rules for citizen fields, you can read it below.  

## REALIZATION  
This API maked with async technology.  
For web-server used aiohttp package, for postgresql used asyncpg package.  

## DEPOYMENT  
If you use ubuntu 18 you can do this actions:  
```  
apt-get update && apt-get upgrade  
apt-get -y install git  
cd /home  
git clone https://github.com/NorthenFox/hasker.git  
apt-get install make  
cd region_analyzer   
make prod  
```

## TESTING  
For testing uses pytest & pytest_async packages  
 
You can run tests:  
```
pytest -v  
```  

# REST METHODS:  

## 1. Post information about citizens  
**Method**: POST  
**url**: /imports  
**body**: json  

Format input json:  
```
{
"citizens": [
    {
    "citizen_id": 1, 
    "town": "Москва",
    "street": "Льва Толстого",
    "building": "16к7стр5",
    "apartment": 7,
    "name": "Иванов Иван Иванович",
    "birth_date": "01.02.2000",
    "gender": "male",
    "relatives": [2, 28] 
    }, ...]
}
```  


Format output message:
```  
HTTP 201
{
  "data": 
    {
      "import_id": 1
    }
}
```  
  
**Information**:  
Validation rules:  
*citizen_id* - int, not NULL, not empty  
*town* - str, not NULL, not empty  
*street* - str, not NULL, not empty  
*building* - str, not NULL, not empty  
*apartment* - int, not NULL, not empty  
*name* - str, not NULL, not empty  
*birth_date* - str, not NULL, not empty, str of date in format "dd.mm.yyyy"  
*gender* - str, not NULL, not empty, can be only "male" or "female"  
*relatives* - list of int, not NULL, may be empty  
All fields must be in package, and not allowed any extra fields.  

If validation is breaking we got 400 error enstead 201.


## 2. Update information of citizen  
**Method**: PATCH  
**url**: /imports/$import_id/citizens/$citizen_id  
**body**: json  

Format input json:  
```
{
"town": "Керчь",
"street": "Иосифа Бродского"
}
```  

Format output message (citizen's info):
```  
HTTP 200
{
  "data": 
    {
      "citizen_id": 1,
      "town": "Керчь",
      "street": "Иосифа Бродского",
      "building": "16к7стр5",
      "apartment": 7,
      "name": "Иванов Иван Иванович",
      "birth_date": "01.02.2000",
      "gender": "male",
      "relatives": [2, 28]
  }
}
``` 

**Information**:  
Validation rules:  
vars from url (import_id & citizen_id) must be int and there must be citizen in DB with that import_id and citizen_id  
  
  
*citizen_id* - int, not NULL, not empty  
*town* - str, not NULL, not empty  
*street* - str, not NULL, not empty  
*building* - str, not NULL, not empty  
*apartment* - int, not NULL, not empty  
*name* - str, not NULL, not empty  
*birth_date* - str, not NULL, not empty, str of date in format "dd.mm.yyyy"  
*gender* - str, not NULL, not empty, can be only "male" or "female"  
*relatives* - list of int, not NULL, may be empty  
Not allowed any extra fields in package.

If validation is breaking we got 400 error enstead 200.  

## 3. Get citizens info with current import_id  
**Method**: GET  
**url**: /imports/$import_id/citizens


Format input json:  
```  
HTTP 200  
{
"data": [
    {
    "citizen_id": 1, 
    "town": "Москва",
    "street": "Льва Толстого",
    "building": "16к7стр5",
    "apartment": 7,
    "name": "Иванов Иван Иванович",
    "birth_date": "01.02.2000",
    "gender": "male",
    "relatives": [2, 28] 
    }, ...]
}
```  

**Information**:  
Validation rules:  
var from url (import_id) must be int and there must be citizens in DB with that import_id  

If validation is breaking we got 400 error enstead 200.  
  
## 4. Get presents calendar for current import_id  
**Method**: GET  
**url**: /imports/$import_id/citizens/birthdays  


Format input json:  
```  
HTTP 200  
{  
  "data": {  
    "1": [  
      {  
        "citizen_id": 1,  
        "presents": 20  
      }],  
    "2": [  
      {   
        "citizen_id": 2,  
        "presents": 7  
      }],  
    "3": [],  
    ...  
    "12": [
      {    
        "citizen_id": 3,  
        "presents": 4  
      }, {  
        "citizen_id": 8,  
        "presents": 2  
      }]  
    }  
}  
```  

**Information**:  
Validation rules:  
var from url (import_id) must be int and there must be citizens in DB with that import_id  

If validation is breaking we got 400 error enstead 200.  

## 5. Get percentile age for different towns  
**Method**: GET  
**url**: /imports/$import_id/towns/stat/percentile/age  


Format input json:  
```  
HTTP 200  
{  
  "data": [  
    {  
      "town": "Москва",  
      "p50": 20,  
      "p75": 45,  
      "p99": 100  
    },  
    {  
      "town": "Санкт-Петербург",  
      "p50": 17,  
      "p75": 35,  
      "p99": 80  
    }  
   ]  
}  
```  

**Information**:  
Validation rules:  
var from url (import_id) must be int and there must be citizens in DB with that import_id  

If validation is breaking we got 400 error enstead 200. 
