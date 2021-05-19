from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas
import pymongo
import pickle
import pmdarima


def process_data():
    temperature = pandas.read_csv('/tmp/p2/temperature.csv')
    humidity = pandas.read_csv('/tmp/p2/humidity.csv')
    temperature_sanfrancisco = temperature['San Francisco']
    humidity_sanfrancisco = humidity['San Francisco']
    datetime = humidity['datetime']
    selected_columns = {'DATE': datetime,
                        'TEMP': temperature_sanfrancisco, 'HUM': humidity_sanfrancisco}
    dataframe = pandas.DataFrame(
        data=selected_columns).dropna().to_dict("records")

    return dataframe


def process_and_store():
    data = process_data()
    client = pymongo.MongoClient('0.0.0.0:27017')
    coleccion = client['forecast']['sanfrancisco']
    coleccion.insert_many(data)


def train_temperature():
    client = pymongo.MongoClient('0.0.0.0:27017')
    collection = client["forecast"]["sanfrancisco"]
    df = pandas.DataFrame(list(collection.find()))

    model_temperature = pmdarima.auto_arima(df['TEMP'], start_p=1, start_q=1,
                                            test='adf',
                                            max_p=3, max_q=3,
                                            m=1,
                                            d=None,
                                            seasonal=False,
                                            start_P=0,
                                            D=0,
                                            trace=True,
                                            error_action='ignore',
                                            suppress_warnings=True,
                                            stepwise=True)
    pickle.dump(model_temperature, open(
        "/tmp/p2/services/model_temperature.pkl", "wb"))


def train_humidity():
    client = pymongo.MongoClient('0.0.0.0:27017')
    collection = client["forecast"]["sanfrancisco"]
    df = pandas.DataFrame(list(collection.find()))
    model_humidity = pmdarima.auto_arima(df['HUM'], start_p=1, start_q=1,
                                         test='adf',
                                         max_p=3, max_q=3,
                                         m=1,
                                         d=None,
                                         seasonal=False,
                                         start_P=0,
                                         D=0,
                                         trace=True,
                                         error_action='ignore',
                                         suppress_warnings=True,
                                         stepwise=True)

    pickle.dump(model_humidity,  open(
        "/tmp/p2/services/model_humidity.pkl", 'wb'))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'practica2',
    default_args=default_args,
    description='Tareas de la práctica 2',
    schedule_interval=timedelta(days=1),
)

Setup = BashOperator(
    task_id='Setup',
    depends_on_past=False,
    bash_command='rm -rf /tmp/p2 && mkdir -p /tmp/p2',
    dag=dag
)

GetHumidity = BashOperator(
    task_id='get_humidity_csv',
    depends_on_past=False,
    bash_command='wget --output-document /tmp/p2/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag
)
GetTemperature = BashOperator(
    task_id='get_temperature_csv',
    depends_on_past=False,
    bash_command='wget --output-document /tmp/p2/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag
)

GetService = BashOperator(
    task_id='get_service',
    depends_on_past=False,
    bash_command='rm -rf /tmp/p2/git && git clone https://github.com/arturocs/CC2_airflow.git /tmp/p2/git && cp -r /tmp/p2/git/services /tmp/p2/',
    dag=dag
)

UnzipHumidity = BashOperator(
    task_id='unzip_humidity',
    depends_on_past=False,
    bash_command='unzip -o /tmp/p2/temperature.csv.zip -d /tmp/p2',
    dag=dag
)

UnzipTemperature = BashOperator(
    task_id='unzip_temperature',
    depends_on_past=False,
    bash_command='unzip -o /tmp/p2/humidity.csv.zip -d /tmp/p2 ',
    dag=dag
)

StartMongo = BashOperator(
    task_id='start_mongo',
    depends_on_past=False,
    bash_command='docker stop $(docker ps -a -q) ; docker run --rm -d -p 27017:27017 mongo:latest',
    dag=dag,
)

ProcessAndStore = PythonOperator(
    task_id='process_and_store',
    depends_on_past=False,
    python_callable=process_and_store,
    dag=dag
)

TrainTemperature = PythonOperator(
    task_id='train_temperature',
    depends_on_past=False,
    python_callable=train_temperature,
    dag=dag
)

TrainHumidity = PythonOperator(
    task_id='train_humidity',
    depends_on_past=False,
    python_callable=train_humidity,
    dag=dag
)

TestAPIs = BashOperator(
    task_id='test_apis',
    depends_on_past=False,
    bash_command='python -m pytest /tmp/p2/services/tests.py && docker stop $(docker ps -a -q)',
    dag=dag
)


RunAPIv1 = BashOperator(
    task_id='run_apiv1',
    depends_on_past=False,
    bash_command='cd /tmp/p2/services && docker build -t apiv1 -f /tmp/p2/services/APIv1.dockerfile . && docker run -d --rm -p 8000:8000 apiv1',
    dag=dag
)

RunAPIv2 = BashOperator(
    task_id='run_apiv2',
    depends_on_past=False,
    bash_command='cd /tmp/p2/services && docker build -t apiv2 -f /tmp/p2/services/APIv2.dockerfile . && docker run -d --rm -p 8001:8001 apiv2',
    dag=dag
)


Setup >> [GetHumidity, GetTemperature, GetService, StartMongo]
GetHumidity >> UnzipHumidity
GetTemperature >> UnzipTemperature
[UnzipHumidity, UnzipTemperature, StartMongo,
    GetService] >> ProcessAndStore >> TrainTemperature >> TrainHumidity >> TestAPIs >> [RunAPIv1, RunAPIv2]
