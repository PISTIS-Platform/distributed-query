from json import dumps
from urllib.parse import urlencode

import jwt
import requests


class DistributedQuery:
    def __init__(self, iam_url, iam_realm, iam_audience, registry_url, catalogue_url, repository_url):
        self.bearer = None
        self.token_url = None
        self.audience = 'resource-server'
        if iam_url is not None and iam_url != '':
            if iam_realm is not None and iam_realm != '':
                self.token_url = '{}/auth/realms/{}/protocol/openid-connect/token'.format(iam_url, iam_realm)
            if iam_audience is not None and iam_audience != '':
                self.audience = iam_audience
        self.registry_url = registry_url
        self.catalogue_url = catalogue_url
        self.repository_url = repository_url

    def forward(self, headers, payload):
        if self.catalogue_url is not None and self.catalogue_url != '':
            url = '{}/search'.format(self.catalogue_url)
            return requests.request('POST', url, headers=headers, data=payload)
        else:
            return None

    def search(self, query, bearer, page=1, limit=10, uuids_brute=True, asset_check=False, iam_bulk=False, catalogue_bulk=False):
        self.bearer = bearer
        factories = self._fetch_factories_urls()
        matches = []
        if factories is not None:
            for f in factories:
                m = self._search_in_factory(f, query, page, limit)
                if m is not None:
                    matches.extend(m)
            matches = list(set(matches))
            if uuids_brute:
                matches = self._fetch_datasets_uuids_brute(matches)
            if asset_check:
                if iam_bulk:
                    matches = self._check_access_rights_bulk(matches)
                else:
                    matches = self._check_access_rights(matches)
            if catalogue_bulk:
                return self._fetch_datasets_metadata_bulk(matches)
            else:
                return self._fetch_datasets_metadata(matches)
        else:
            return []

    def _fetch_factories_urls(self):
        if self.registry_url is not None and self.registry_url != '':
            url = '{}/api/factories/list'.format(self.registry_url)
            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.bearer
            }
            response = requests.request('GET', url, headers=headers)
            if response.status_code == 200:
                return response.json()
            return None
        else:
            return None

    def _search_in_factory(self, factory, query, page=1, limit=10):
        url = '{}/srv/distributed-query/search?page={}&limit={}'.format(factory, page, limit)
        payload = {'query': query}
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.bearer
        }
        response = requests.request('POST', url, headers=headers, data=dumps(payload))
        if response.status_code == 200:
            r = response.json()
            if r['success']:
                return r['matches']
        return None

    def _fetch_datasets_uuids_brute(self, matches):
        datasets_uuids = []
        datasets_map = self._get_datasets_map()
        for d in datasets_map:
            for uuid in datasets_map[d]:
                if uuid in matches:
                    datasets_uuids.append(d)
                    break
        return datasets_uuids

    def _check_access_rights(self, matches):
        filtered_matches = []
        if self.token_url is not None:
            for m in matches:
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': self.bearer
                }
                payload = {
                    'grant_type': 'urn:ietf:params:oauth:grant-type:uma-ticket',
                    'audience': self.audience,
                    'permission': m
                }
                encoded_payload = '{}#READ'.format(urlencode(payload))
                response = requests.request('POST', self.token_url, headers=headers, data=encoded_payload)
                if response.status_code == 200:
                    filtered_matches.append(m)
        return filtered_matches

    def _check_access_rights_bulk(self, matches):
        filtered_matches = []
        if self.token_url is not None:
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': self.bearer
            }
            payload = {
                'grant_type': 'urn:ietf:params:oauth:grant-type:uma-ticket',
                'audience': self.audience
            }
            encoded_payload = urlencode(payload)
            for m in matches:
                encoded_payload = '{}&permission={}'.format(encoded_payload, m)
            response = requests.request('POST', self.token_url, headers=headers, data=encoded_payload)
            if response.status_code == 200:
                filtered_matches = self._parse_assets_authorization_response(response.json(), matches)
        return filtered_matches

    def _fetch_datasets_metadata(self, matches):
        datasets = []
        if self.catalogue_url is not None and self.catalogue_url != '':
            for m in matches:
                url = '{}/datasets/{}'.format(self.catalogue_url, m)
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': self.bearer
                }
                response = requests.request('GET', url, headers=headers)
                if response.status_code == 200:
                    r = response.json()
                    if 'result' in r:
                        datasets.append(r['result'])
        return datasets

    def _fetch_datasets_metadata_bulk(self, matches):
        if self.catalogue_url is not None and self.catalogue_url != '':
            url = '{}/search'.format(self.catalogue_url)
            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.bearer
            }
            payload = {
                'filter': 'dataset',
                'facets': {
                    'id': matches
                }
            }
            response = requests.request('POST', url, headers=headers, data=dumps(payload))
            if response.status_code == 200:
                r = response.json()
                return r['result']['results']
        return []

    def _get_datasets_map(self):
        datasets_map = {}
        for dataset in self._fetch_all_datasets():
            if '@id' not in dataset or '@graph' not in dataset:
                continue
            dataset_uuid = dataset['@id'].split('/')[-1]
            dataset_graph = dataset['@graph']
            dataset_files = []
            for v in dataset_graph:
                if 'dcat:accessURL' in v:
                    if '@id' in v['dcat:accessURL']:
                        file_uuid = v['dcat:accessURL']['@id'].split('asset_uuid=')[-1]
                        dataset_files.append(file_uuid)
            if len(dataset_files) > 0:
                datasets_map[dataset_uuid] = dataset_files
        return datasets_map

    def _fetch_all_datasets(self):
        if self.repository_url is not None and self.repository_url != '':
            url = '{}/datasets?valueType=metadata'.format(self.repository_url)
            headers = {
                'Authorization': self.bearer
            }
            response = requests.request('GET', url, headers=headers)
            if response.status_code == 200:
                r = response.json()
                if '@graph' in r:
                    return r['@graph']
        return []

    @staticmethod
    def _parse_assets_authorization_response(response, matches):
        filtered_matches = []
        payload = jwt.decode(response['access_token'], options={'verify_signature': False})
        if 'authorization' in payload:
            if 'permissions' in payload['authorization']:
                for p in payload['authorization']['permissions']:
                    if p['rsid'] in matches and 'READ' in p['scopes']:
                        filtered_matches.append(p['rsid'])
        return filtered_matches
