import json
import requests
from datetime import datetime
from flask import Flask, Response


app = Flask(__name__)


def predict(time):
    url = f"http://api.weatherapi.com/v1/forecast.json?key=64a6a1f0619944a687d123221211705&q=San Francisco&days={time}"
    response = requests.get(url)
    prediction = response.json()['forecast']['forecastday']
    resultado = []
    for days in prediction:
        for hour in days['hour']:
            resultado.append(
                {'hour': datetime.utcfromtimestamp(hour['time_epoch']).strftime('%d-%m %H:%M'),
                 'temp': hour['temp_c'], 'hum': (hour['humidity'])})

    return resultado


@app.route("/servicio/v2/prediccion/24horas", methods=['GET'])
def hours_24():
    response = Response(json.dumps(predict(1)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route("/servicio/v2/prediccion/48horas", methods=['GET'])
def hours_48():
    response = Response(json.dumps(predict(2)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route("/servicio/v2/prediccion/72horas", methods=['GET'])
def hours_72():
    response = Response(json.dumps(predict(2)), status=200)
    response.headers['Content-Type'] = 'application/json'
    return response
