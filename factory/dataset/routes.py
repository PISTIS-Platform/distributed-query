from json import dumps

import requests
from flask import Blueprint, request

from dataset.indexing import *
from shared import config, token_required

dataset_blueprint = Blueprint('dataset_blueprint', __name__)


@dataset_blueprint.route('/dataset/<uuid:dataset_id>', methods=['POST', 'PUT', 'DELETE'])
@token_required
def search_data(dataset_id):
    if request.method == 'POST':
        return insert_dataset(dataset_id)
    elif request.method == 'PUT':
        return update_dataset(dataset_id)
    elif request.method == 'DELETE':
        return delete_dataset(dataset_id)
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


def insert_dataset(dataset_id):
    data = fetch_dataset(dataset_id)
    if data is not None:
        if create_hashes(dataset_id, data):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def update_dataset(dataset_id):
    data = fetch_dataset(dataset_id)
    if data is not None:
        if update_hashes(dataset_id, data):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def delete_dataset(dataset_id):
    if delete_hashes(dataset_id):
        return dumps({'success': True})
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


def fetch_dataset(dataset_id):
    data = None
    if config['ds_url'] is not None and config['ds_url'] != '':
        url = '{}/api/tables/get_table?asset_uuid={}'.format(config['ds_url'], dataset_id)
        headers = {
            'Authorization': config['app']['token']
        }
        response = requests.request('GET', url, headers=headers)
        if response.status_code == 200:
            tables = []
            for t in response.json():
                rows = []
                for row in t['data']['rows']:
                    rows.append('\t'.join(row))
                tables.append('\n'.join(rows))
            data = '\n'.join(tables)
        else:
            print('{}: {}'.format(response.status_code, response.text))
    return data
