from json import dumps

import requests
from flask import Blueprint, request

from dataset.indexing import *
from shared import config, token_required

dataset_blueprint = Blueprint('dataset_blueprint', __name__)


@dataset_blueprint.route('/dataset/<uuid:dataset_id>', methods=['POST', 'PUT'])
@token_required
def index_dataset(dataset_id):
    bearer = request.headers['Authorization']
    dataset_type = request.args.get('dataset_type', default='file', type=str)
    if request.method == 'POST':
        return insert_dataset(dataset_id, bearer, dataset_type)
    elif request.method == 'PUT':
        return update_dataset(dataset_id, bearer, dataset_type)
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


@dataset_blueprint.route('/dataset/<uuid:dataset_id>', methods=['DELETE'])
@token_required
def delete_dataset(dataset_id):
    if request.method == 'DELETE':
        if delete_hashes(dataset_id):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


def insert_dataset(dataset_id, bearer, dataset_type='file'):
    data = fetch_dataset_file(dataset_id, bearer, dataset_type)
    if data is not None:
        if create_hashes(dataset_id, data):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def update_dataset(dataset_id, bearer, dataset_type='file'):
    data = fetch_dataset_file(dataset_id, bearer, dataset_type)
    if data is not None:
        if update_hashes(dataset_id, data):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def fetch_dataset_file(dataset_id, bearer, dataset_type='file'):
    data = None
    if config['ds_url'] is not None and config['ds_url'] != '':
        url = '{}/api/files/get_file?asset_uuid={}'.format(config['ds_url'], dataset_id)
        headers = {
            'Authorization': bearer
        }
        response = requests.request('GET', url, headers=headers)
        if response.status_code == 200:
            if dataset_type == 'tables':
                tables = []
                for t in response.json():
                    rows = []
                    for row in t['data']['rows']:
                        rows.append('\t'.join(row))
                    tables.append('\n'.join(rows))
                data = '\n'.join(tables)
            else:
                data = response.text
        else:
            print('{}: {}'.format(response.status_code, response.text))
    return data
