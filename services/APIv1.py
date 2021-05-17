import json
import pandas
import time
from datetime import datetime
from flask import Flask, Response
from statsmodels.tsa.arima_model import ARIMA
import pickle

app = Flask(__name__)


def predict(t):
    temperature_model = pickle.load(
        open("/tmp/p2/services/model_temperature.pkl", "rb"))
    humidity_model = pickle.load(
        open("/tmp/p2/services/model_humidity.pkl", "rb"))
    temperature_prediction, _ = temperature_model.predict(
        n_periods=t, return_conf_int=True)
    humidity_prediction, _ = humidity_model.predict(
        n_periods=t, return_conf_int=True)
    date_range = pandas.date_range(datetime.now().replace(
        second=0, microsecond=0), periods=t, freq='H')
    resultado = [{'hour': datetime.utcfromtimestamp(time.mktime(t.timetuple())).strftime(
        '%d-%m %H:%M'), 'temp': temp, 'hum': hum} for t, temp, hum in zip(date_range, temperature_prediction, humidity_prediction)]
    return resultado


@app.route("/servicio/v1/prediccion/24horas", methods=['GET'])
def hours_24():
    response = Response(json.dumps(predict(24)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route("/servicio/v1/prediccion/48horas", methods=['GET'])
def hours_48():
    response = Response(json.dumps(predict(48)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route("/servicio/v1/prediccion/72horas", methods=['GET'])
def hours_72():
    response = Response(json.dumps(predict(72)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response
