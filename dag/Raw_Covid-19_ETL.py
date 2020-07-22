import airflow
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import boto3
from io import StringIO

def data_toS3(*args, **kwargs):
    day = {kwargs['ds']}.pop()
    day = datetime.strptime(day, '%Y-%m-%d').date() - timedelta(days=2)
    day = day.strftime("%m-%d-%Y")
    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
          'csse_covid_19_daily_reports/'+day+'.csv'
    df = pd.read_csv(url, error_bad_lines=False)

    columnQuery = ['Admin2',
                   'Province',
                   'Country',
                   'Update',
                   'Confirmed',
                   'Deaths',
                   'Recovered']

    column_names = df.columns.to_list()
    column_notfound = []
    for column in columnQuery:
        column_found = next((s for s in column_names if column in s), None)
        if column_found is not None:
            df = df.rename(columns={column_found: column})
        else:
            column_notfound.append(column)

    for column in column_notfound:
        df[column] = np.nan

    df = df[columnQuery]
    df['Update'] = df['Update'].astype('datetime64[ns]')
    df["Update"] = df["Update"].dt.strftime("%Y-%m-%d")
    df = df.groupby(by='Update').filter(lambda x: len(x) > 10)

    df = df.rename(columns={'Admin2': 'City', 'Update':'Last_Update'})
    df["City"] = df["City"].fillna('N/A')
    df["Province"] = df["Province"].fillna('N/A')
    df["Country"] = df["Country"].fillna('N/A')

    df["Confirmed"] = df["Confirmed"].fillna(0)
    df["Deaths"] = df["Deaths"].fillna(0)
    df["Recovered"] = df["Recovered"].fillna(0)
    df = df.astype({"Confirmed": int, "Deaths": int, "Recovered": int})

    aws_hook = AwsHook(aws_conn_id="aws_credentials")
    credentials = aws_hook.get_credentials()
    s3resource = boto3.resource('s3',
                      aws_access_key_id=credentials.access_key,
                      aws_secret_access_key=credentials.secret_key)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False, sep=';')
    s3resource.Object('larry-covid-data', 'Covid-19-' + day + '.csv').put(Body=csv_buffer.getvalue())
    return

def S3_toRedShift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")

    day = {kwargs['ds']}.pop()
    day = datetime.strptime(day, '%Y-%m-%d').date() - timedelta(days=2)
    day = day.strftime("%m-%d-%Y")

    sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ';'
        TIMEFORMAT 'auto'
        """
    redshift_hook.run(sql.format("main",
                                 "s3://larry-covid-data/Covid-19-" + day + ".csv",
                                 credentials.access_key,
                                 credentials.secret_key))
    return

def insertUSdata(*args, **kwargs):
    # aws_hook = AwsHook("aws_credentials")
    # credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")

    day = {kwargs['ds']}.pop()
    day = datetime.strptime(day, '%Y-%m-%d').date() - timedelta(days=2)
    day = day.strftime("%Y-%m-%d")

    sql = """
        INSERT INTO {}
        SELECT 
        City,
        Province,
        Country,
        Last_Update,
        Confirmed,
        Deaths,
        Recovered,
        CASE 
            WHEN Confirmed > 0 THEN Deaths/Confirmed * 100
            ELSE 0 
            END Case_Fatality_Ratio
        FROM main
        WHERE Country = 'US' and Last_Update = '""" + day + "'"
    redshift_hook.run(sql.format("US_only"))
    return

default_args = {
    'owner': 'lzhou519',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 24),
    'email': ['lazhou519@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'Raw_Covid-19_ETL',
    default_args = default_args,
    description = 'Raw Covid-19 ETL from https://github.com/CSSEGISandData/COVID-19',
    schedule_interval='0 6 * * *')

create_table_main = PostgresOperator(
    task_id="create_table_main",
    dag=dag,
    postgres_conn_id="redshift",
    sql=""" CREATE TABLE IF NOT EXISTS main (
            City VARCHAR,
            Province VARCHAR,
            Country VARCHAR,
            Last_Update DATE,
            Confirmed INTEGER,
            Deaths INTEGER,
            Recovered INTEGER);""")

create_table_us = PostgresOperator(
    task_id="create_table_us",
    dag=dag,
    postgres_conn_id="redshift",
    sql=""" CREATE TABLE IF NOT EXISTS US_only (
            City VARCHAR,
            Province VARCHAR,
            Country VARCHAR,
            Last_Update DATE,
            Confirmed INTEGER,
            Deaths INTEGER,
            Recovered INTEGER,
            Case_Fatality_Ratio float);""")

MovedataToS3 = PythonOperator(
    task_id="data_toS3",
    provide_context=True,
    python_callable=data_toS3,
    dag=dag
)

MovetoRedShift = PythonOperator(
    task_id="S3_toRedShift",
    provide_context=True,
    python_callable=S3_toRedShift,
    dag=dag
)

insert_USdata = PythonOperator(
    task_id="insert_USdata",
    provide_context=True,
    python_callable=insertUSdata,
    dag=dag
)

create_table_main >> create_table_us >> MovedataToS3 >> MovetoRedShift >> insert_USdata

