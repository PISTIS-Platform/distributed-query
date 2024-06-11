#!/bin/bash

tee /pistis-dq-orchestrator/config.json > /dev/null <<EOT
{
  "app": {
    "host": "0.0.0.0",
    "port": 5000
  },
  "iam": {
    "url": "${IAM_URL}",
    "realm": "${IAM_REALM}",
    "public_key": "${IAM_PK}",
    "jwt_local": ${JWT_LOCAL},
    "audience": ${IAM_AUDIENCE}
  },
  "registry": "${REGISTRY_URL}",
  "catalogue": "${CATALOGUE_URL}"
}
EOT

export PYTHONPATH=/usr/lib/python3/dist-packages/

python -u /pistis-dq-orchestrator/app.py