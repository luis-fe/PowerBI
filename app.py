import pandas as pd
from flask import Flask, render_template, jsonify, request, send_from_directory
#from flask_cors import CORS
import os
from werkzeug.utils import secure_filename
from functools import wraps



app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))

#app.register_blueprint(routes_blueprint)

def token_required(f): #Aqui passamos o token fixo, que pode ser alterado
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
@app.route('/PowerBI/api/teste', methods=['GET'])
def teste():
    usuarios = pd.DataFrame([{'nome':'teste'}])
    # Obtém os nomes das colunas

    column_names = usuarios.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in usuarios.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
