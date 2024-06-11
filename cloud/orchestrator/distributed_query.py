from json import dumps
from urllib.parse import urlencode

import jwt
import requests


class DistributedQuery:
    def __init__(self, iam_url, iam_realm, registry_url, catalogue_url):
        self.bearer = None
        self.token_url = None
        if iam_url is not None and iam_url != '':
            if iam_realm is not None and iam_realm != '':
                self.token_url = '{}/auth/realms/{}/protocol/openid-connect/token'.format(iam_url, iam_realm)
        self.registry_url = registry_url
        self.catalogue_url = catalogue_url

    def search(self, query, bearer, page=1, limit=10, bulk_check=True):
        self.bearer = bearer
        factories = self._fetch_factories_urls()
        matches = []
        if factories is not None:
            for f in factories:
                m = self._search_in_factory(f, query, page, limit)
                if m is not None:
                    matches.extend(m)
            if bulk_check:
                filtered_matches = self._check_access_rights(matches)
            else:
                filtered_matches = self._check_access_rights_bulk(matches)
            datasets = self._fetch_datasets_metadata(filtered_matches)
            return datasets
        else:
            return []

    def forward(self, headers, payload):
        if self.catalogue_url is not None and self.catalogue_url != '':
            url = '{}/search'.format(self.catalogue_url)
            return requests.request('POST', url, headers=headers, data=payload)
        else:
            return None

    def _fetch_factories_urls(self):
        if self.registry_url is not None and self.registry_url == '':
            #TODO: Retrieve factories list from the Registry
            return []
        else:
            return None

    def _search_in_factory(self, factory, query, page=1, limit=10):
        url = '{}/search?page={}&limit={}'.format(factory, page, limit)
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
                    'audience': 'pistis-resource-server',
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
                'audience': 'pistis-resource-server'
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
                url = '{}/search/datasets/{}'.format(self.catalogue_url, m)
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': self.bearer
                }
                response = requests.request('POST', url, headers=headers)
                datasets.append(response.json())
        return datasets

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
