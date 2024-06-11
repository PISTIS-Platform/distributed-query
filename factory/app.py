from flask import Flask
from flask_cors import CORS

from dataset.routes import dataset_blueprint
from search.routes import search_blueprint
from shared import config

app = Flask(__name__)
app.secret_key = 'PFtdbiPdkJevKACdS2eyVMsuKNCVRObt'
app.config['DEBUG'] = True

CORS(app)

app.register_blueprint(dataset_blueprint)
app.register_blueprint(search_blueprint)


@app.route('/', methods=['GET'])
def home():
    return '<h1>PISTIS Distributed Query API</h1>'


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


if __name__ == '__main__':
    app.run(host=config['app']['host'], port=config['app']['port'])
