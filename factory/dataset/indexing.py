import re
from datasketch import MinHash, MinHashLSH

from shared import config


storage_config = {
    'type': 'redis',
    'redis': {'host': config['db']['host'], 'port': config['db']['port']}
}
lsh = MinHashLSH(threshold=config['lsh']['threshold'], num_perm=config['lsh']['size'], storage_config=storage_config)


def preprocess_text(text):
    text = re.sub(r'[^\w\s]', '', text)
    tokens = text.lower()
    tokens = tokens.split()
    return tokens


def create_hashes(dataset_id, data):
    mh = MinHash(num_perm=config['lsh']['size'])
    tokens = preprocess_text(data)
    for t in tokens:
        mh.update(t.encode('utf-8'))
    # Create LSH index
    lsh.insert(dataset_id, mh)
    return True


def update_hashes(dataset_id, data):
    if delete_hashes(dataset_id):
        if create_hashes(dataset_id, data):
            return True
    return False


def delete_hashes(dataset_id):
    flag = False
    try:
        lsh.remove(dataset_id)
        flag = True
    except ValueError:
        print('Dataset id does not exist')
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
    for uuid in results:
        serializable_results.append(str(uuid))
    return serializable_results
