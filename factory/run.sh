#!/bin/bash

tee /pistis-dq-lsh/config.json > /dev/null <<EOT
{
  "app": {
    "host": "0.0.0.0",
    "port": 5000,
    "token": "${APP_TOKEN}"
  },
  "lsh": {
    "size": ${LSH_SIZE},
    "threshold": ${LSH_THRESHOLD}
  },
  "db": {
    "host": "${DB_HOST}",
    "port": "${DB_PORT}",
    "username": "${DB_USERNAME}",
    "password": "${DB_PASSWORD}"
  },
  "iam": {
    "url": "${IAM_URL}",
    "realm": "${IAM_REALM}",
    "public_key": "${IAM_PK}",
    "jwt_local": ${JWT_LOCAL},
    "audience": "${IAM_AUDIENCE}"
  },
  "ds_url": "${DS_URL}",
  "cat_url": "${CAT_URL}",
  "repo_url": "${REPO_URL}",
  "dist_url": "${DIST_URL}"
}
EOT

export PYTHONPATH=/usr/lib/python3/dist-packages/

python -u /pistis-dq-lsh/app.py