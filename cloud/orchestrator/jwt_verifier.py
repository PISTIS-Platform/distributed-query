from urllib.parse import urlencode

import jwt
import requests
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError, InvalidAudienceError


class JWTVerifier:
    def __init__(self, iam_url, iam_realm, iam_pk, jwt_local=True, audience=None):
        self.realm_url = None
        self.token_url = None
        self.pk = None
        self.jwt_local = None
        self.audience = None
        if iam_url is not None and iam_url != '':
            if iam_realm is not None and iam_realm != '':
                self.realm_url = '{}/auth/realms/{}'.format(iam_url, iam_realm)
                self.token_url = '{}/protocol/openid-connect/token'.format(self.realm_url)
                self.jwt_local = bool(jwt_local)
                if iam_pk is not None and iam_pk != '':
                    self.pk = iam_pk
                if audience is not None and audience != '':
                    self.audience = audience
        if self.realm_url is None:
            raise ValueError('IAM parameters are missing')

    def verify(self, token):
        if self.jwt_local:
            return self.verify_locally(token)
        else:
            return self.verify_remotely(token)

    def verify_locally(self, token):
        if token is None:
            return False
        if self.pk is None:
            if not self.fetch_public_key():
                print('failed')
                return False
        token = token.replace('Bearer ', '')
        header_data = jwt.get_unverified_header(token)
        try:
            if self.audience is None:
                jwt.decode(token, key=self.pk, algorithms=[header_data['alg']], options={'verify_aud': False})
            else:
                jwt.decode(token, key=self.pk, algorithms=[header_data['alg']], audience=self.audience)
        except (AttributeError, ValueError, ExpiredSignatureError, InvalidSignatureError, InvalidAudienceError) as e:
            print(str(e))
            return False
        return True

    def verify_remotely(self, token):
        if token is not None:
            token = token.replace('Bearer ', '')
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': token
            }
            payload = {
                'grant_type': 'urn:ietf:params:oauth:grant-type:uma-ticket',
                'audience': 'pistis-resource-server'
            }
            response = requests.request('POST', self.token_url, headers=headers, data=urlencode(payload))
            if response.status_code == 200:
                return True
        return False

    def fetch_public_key(self, pem_line_length=64):
        response = requests.request('GET', self.realm_url)
        if response.status_code == 200:
            r = response.json()
            if 'public_key' in r:
                self.pk = '-----BEGIN PUBLIC KEY-----'
                for i in range(0, len(r['public_key']), pem_line_length):
                    new_line = r['public_key'][i:(i + pem_line_length)]
                    self.pk = '{}\n{}'.format(self.pk, new_line)
                self.pk = '{}\n-----END PUBLIC KEY-----\n'.format(self.pk)
                return True
        return False
