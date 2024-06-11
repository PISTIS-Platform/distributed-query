from json import dumps

from flask import Blueprint, request

from dataset.indexing import search_hashes
from shared import token_required

search_blueprint = Blueprint('search_blueprint', __name__)


@search_blueprint.route('/search', methods=['POST'])
@token_required
def search_dataset():
    if request.is_json:
        req = request.get_json()
        if 'query' in req:
            return dumps({'success': True, 'matches': search_hashes(req['query'])})
        else:
            return dumps({'success': False, 'error': 'No query was provided'}), 400
    else:
        return dumps({'success': False, 'error': 'Request type must be JSON'}), 400
