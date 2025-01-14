from json import dumps

import requests
from flask import Blueprint, request

from dataset.indexing import *
from shared import config, token_required

dataset_blueprint = Blueprint('dataset_blueprint', __name__)


@dataset_blueprint.route('/dataset/<uuid:distribution_id>', methods=['POST', 'PUT'])
@token_required
def index_dataset(distribution_id):
    bearer = request.headers['Authorization']
    dataset_type = request.args.get('dataset_type', default='file', type=str)
    if request.method == 'POST':
        return insert_dataset(distribution_id, bearer, dataset_type)
    elif request.method == 'PUT':
        return update_dataset(distribution_id, bearer, dataset_type)
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


@dataset_blueprint.route('/dataset/<uuid:distribution_id>', methods=['DELETE'])
@token_required
def delete_dataset(distribution_id):
    if request.method == 'DELETE':
        if delete_hashes(distribution_id):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Unknown error'}), 500


def insert_dataset(distribution_id, bearer, dataset_type='file'):
    asset_id = fetch_asset_id(distribution_id, bearer)
    dataset_id = fetch_dataset_id(distribution_id, bearer)
    data = fetch_dataset_file(asset_id, bearer, dataset_type)
    if data is not None:
        if create_hashes(distribution_id, data, dataset_id):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def update_dataset(distribution_id, bearer, dataset_type='file'):
    data = fetch_dataset_file(distribution_id, bearer, dataset_type)
    if data is not None:
        if update_hashes(distribution_id, data):
            return dumps({'success': True})
        else:
            return dumps({'success': False, 'error': 'Unknown error'}), 500
    else:
        return dumps({'success': False, 'error': 'Could not fetch dataset'}), 500


def fetch_dataset_id(distribution_id, bearer):
    dataset_id = None
    if config['cat_url'] is not None and config['cat_url'] != '':
        query_prefix = 'PREFIX dcat: <http://www.w3.org/ns/dcat%23> PREFIX dct: <http://purl.org/dc/terms/>'
        query_clause = 'dcat:distribution <{}/{}> . '.format(config['dist_url'], distribution_id)
        dataset_uri = execute_sparql_query(query_prefix, query_clause, bearer)
        dataset_id = dataset_uri.split('/')[-1]
    return dataset_id


def fetch_asset_id(distribution_id, bearer):
    url = '{}/distributions/{}'.format(config['repo_url'], distribution_id)
    headers = {
        'Authorization': bearer
    }
    response = requests.request('GET', url, headers=headers)
    if response.status_code == 200:
        r = response.json()
        if 'dcat:accessURL' in r:
            if '@id' in r['dcat:accessURL']:
                access_uri = r['dcat:accessURL']['@id']
                return access_uri.split('/')[-1]
    else:
        print('{}: {}'.format(response.status_code, response.text))
        return None


def fetch_distribution_id(asset_id, bearer):
    distribution_id = None
    if config['cat_url'] is not None and config['cat_url'] != '':
        query_prefix = 'PREFIX dcat: <http://www.w3.org/ns/dcat%23> PREFIX dct: <http://purl.org/dc/terms/>'
        query_clause = 'dcat:accessURL <{}/api/files/get_file?asset_uuid= . '.format(config['ds_url'], asset_id)
        distribution_uri = execute_sparql_query(query_prefix, query_clause, bearer)
        distribution_id = distribution_uri.split('/')[-1]
    return distribution_id


def execute_sparql_query(query_prefix, query_clause, bearer):
    query = '{} SELECT ?x WHERE {{ ?x {} }} '.format(query_prefix, query_clause)
    url = '{}/sparql?query={}&format=application/json&timeout=0&signal_void=on'.format(config['cat_url'], query)
    headers = {
        'Authorization': bearer
    }
    response = requests.request('GET', url, headers=headers)
    if response.status_code == 200:
        r = response.json()
        if 'results' in r:
            if 'bindings' in r['results'] and len(r['results']['bindings']) > 0:
                return r['results']['bindings'][0]['x']['value']
    else:
        print('{}: {}'.format(response.status_code, response.text))
        return None


def fetch_dataset_file(distribution_id, bearer, dataset_type='file'):
    data = None
    if config['ds_url'] is not None and config['ds_url'] != '':
        url = '{}/api/files/get_file?asset_uuid={}'.format(config['ds_url'], distribution_id)
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
