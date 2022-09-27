from importlib.metadata import files
from os import pipe
from  . import executeNotificationClient
import pandas as pd
from fristfile import main as firstmain
from secondfile import main as secondmain
from thirdfile import main as thridmain
from . import send_email

def poll(app_prefix, pipeline_name, database_name, table_name):
    res = executeNotificationClient(action="poll", message = f'''[{"app_prefix":{app_prefix}, "pipeline":{pipeline_name}, "db":{database_name}, "table":{table_name}}]''', app_prefix = app_prefix, pipeline = pipeline_name, messageType = "transformation",
    subject = "test subject")
    print("Done Polling")
    print(res)
    return [res['body'][table_name]['lastUpdated'][0], res['body'][table_name]['latestStatus']] #['2022-09-22T15:09:10','SUCCESS']


if __name__ == "__main__":
    df = pd.read_csv("file.csv")
    env = ""
    spark = sparkcontent()
    success = []
    failure = []
    for i in range(df.shape[0]): #10
        result = poll(df['App Prefix'].iloc[i], df['Pipeline'].iloc[i], df['Database'].iloc[i], df['Table'].iloc[i])
        if result[1] == "SUCCESS":
            success.append(result)
        else:
            failure.append(result)
    if len(failure) == 0:
        #run three files
    else:
        #send email code. 