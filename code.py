from importlib.metadata import files
from os import pipe
from  . import executeNotificationClient
import pandas as pd
from fristfile import main as firstmain
from secondfile import main as secondmain
from thirdfile import main as thridmain
from . import send_email

from config import tables

def poll(appPrefix, pipeline, database, table):
    res = executeNotificationClient(action="poll", message = '''[{"app_prefix":{app_prefix}, "pipeline":{pipeline_name}, "db":{database_name}, "table":{table_name}}]'''.format(app_prefix=appPrefix, pipeline_name = pipeline, database_name = database, table_name = table), app_prefix = appPrefix, pipeline = pipeline, messageType = "transformation",
    subject = "test subject")
    print("Done Polling")
    print(res)
    return [res['body'][table]['lastUpdated'][0], res['body'][table]['latestStatus']] #['2022-09-22T15:09:10','SUCCESS']


if __name__ == "__main__":
    
    success = []
    failure = []
    for key in tables.keys():
        result = poll(tables[key]['app_prefix'], tables[key]['pipeline'], tables[key]['Database'], key)
        if result[1] == "SUCCESS":
            success.append(result)
        else:
            failure.append(result)
    if len(failure) == 0:
        #Run your Three Files Here
    else:
        #send email code. 