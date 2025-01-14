import re

import redis
from datasketch import MinHash, MinHashLSH

from shared import config


redis_client = None
storage_config = None
if 'db' in config:
    username = None
    password = None
    if 'username' in config['db'] and 'password' in config['db']:
        if config['db']['username'] != '' and config['db']['password'] != '':
            username = config['db']['username']
            password = config['db']['password']
    if 'host' in config['db'] and 'port' in config['db']:
        if config['db']['host'] != '':
            redis_client = redis.Redis(host=config['db']['host'], port=config['db']['port'],
                                       username=username, password=password, decode_responses=True)
            storage_config = {
                'type': 'redis',
                'redis': {
                    'host': config['db']['host'], 'port': config['db']['port'],
                    'username': username, 'password': password
                }
            }
lsh = MinHashLSH(threshold=config['lsh']['threshold'], num_perm=config['lsh']['size'], storage_config=storage_config)


def preprocess_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    tokens = text.lower()
    tokens = tokens.split()
    return tokens


def create_hashes(distribution_id, data, dataset_id=None):
    mh = MinHash(num_perm=config['lsh']['size'])
    tokens = preprocess_text(data)
    for t in tokens:
        mh.update(t.encode('utf-8'))
    # Create LSH index
    lsh.insert(distribution_id, mh)
    print(dataset_id)
    if dataset_id is not None:
        mapping_id = 'map_{}'.format(distribution_id)
        print(mapping_id)
        redis_client.set(mapping_id, dataset_id)
    return True


def update_hashes(dataset_id, data):
    if delete_hashes(dataset_id):
        if create_hashes(dataset_id, data):
            return True
    return False


def delete_hashes(distribution_id, dataset=False):
    flag = False
    try:
        lsh.remove(distribution_id)
        if dataset:
            mapping_id = 'map_{}'.format(distribution_id)
            redis_client.delete(mapping_id)
        flag = True
    except ValueError:
        print('Distribution id does not exist')
        flag = True
    finally:
        return flag


def search_hashes(query):
    mh = MinHash(num_perm=config['lsh']['size'])
    tokens = preprocess_text(query)
    for t in tokens:
        mh.update(t.encode('utf-8'))
    results = lsh.query(mh)
    serializable_results = []
    for distribution_id in results:
        # mapping_id = 'map_{}'.format(distribution_id)
        # dataset_id = redis_client.get(mapping_id)
        serializable_results.append(str(distribution_id))
    return serializable_results
