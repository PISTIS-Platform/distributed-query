import sys
from functools import wraps
from json import load, dumps

from flask import Flask, request, Response
from flask_cors import CORS

from distributed_query import DistributedQuery
from jwt_verifier import JWTVerifier

app = Flask(__name__)
app.secret_key = 'PFtdbiPdkJevKACdS3eyVMsuKNCVRObt'
app.config['DEBUG'] = True
CORS(app)

with open('config.json') as fp:
    config = load(fp)
try:
    app_host = config['app']['host']
    app_port = config['app']['port']
    iam = config['iam']
    jwt = JWTVerifier(iam['url'], iam['realm'], iam['public_key'], iam['jwt_local'], iam['audience'])
    dq = DistributedQuery(iam['url'], iam['realm'], config['registry'], config['catalogue'])
except (AttributeError, ValueError) as e:
    sys.exit(str(e))


def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        if 'Authorization' in request.headers:
            if jwt.verify(request.headers['Authorization']):
                return f(*args, **kwargs)
            else:
                return dumps({'success': False, 'error': 'Unauthorized'}), 401
        else:
            return dumps({'success': False, 'error': 'Unauthorized'}), 401

    return decorator


@app.route('/search', methods=['POST'])
@token_required
def search_dataset():
    if request.is_json:
        bearer = request.headers['Authorization']
        req = request.get_json()
        if 'dataQuery' in req:
            resp = dq.search(req['dataQuery'], bearer)
            if resp is not None:
                return dumps({'success': True, 'datasets': resp})
            else:
                dumps({'success': False, 'error': 'Undefined error'}), 500
        else:
            resp = dq.forward(request.headers, req)
            if resp is not None:
                return Response(resp.text, status=resp.status_code, content_type=resp.headers['content-type'])
            else:
                dumps({'success': False, 'error': 'Undefined error'}), 500
    else:
        return dumps({'success': False, 'error': 'Request type must be JSON'}), 400


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
    app.run(host=app_host, port=app_port)
