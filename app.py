import pandas as pd
from flask import Flask, jsonify, request
from functools import wraps
from service import ordemProdService
from apscheduler.schedulers.background import BackgroundScheduler
import os
import locale
import pytz


import datetime


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%H')
    return hora_str


app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

def execute_periodically():
    print("Executando a cada 15 minutos...")
    usuarios = pd.DataFrame([{'nome': 'teste'}])


    hora = obterHoraAtual()

    if hora in ['10','11','12','13','14','15']:
        ordemProdService.ConjuntodeOP('1')
    else:
        print(hora)
    column_names = usuarios.columns
    OP_data = []
    for index, row in usuarios.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    print(OP_data)

# Configurar o agendador
scheduler = BackgroundScheduler()
scheduler.add_job(execute_periodically, 'interval', minutes=15)
scheduler.start()

if __name__ == '__main__':
   # ordemProdService.ConjuntodeOP('1')
    hora = obterHoraAtual()
    print(hora)
    app.run(host='0.0.0.0', port=port)
