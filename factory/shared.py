import sys
from functools import wraps
from json import load, dumps

from flask import request

from jwt_verifier import JWTVerifier

with open('config.json') as fp:
    config = load(fp)
try:
    iam = config['iam']
    jwt = JWTVerifier(iam['url'], iam['realm'], iam['public_key'], iam['jwt_local'], iam['audience'])
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
