FROM python:3.11

ENV IAM_URL=""
ENV IAM_REALM=""
ENV IAM_PK=""
ENV IAM_AUDIENCE=""
ENV JWT_LOCAL="true"
ENV REGISTRY_URL=""
ENV CATALOGUE_URL=""

WORKDIR /pistis-dq-orchestrator
COPY . /pistis-dq-orchestrator

RUN pip install -r requirements.txt

RUN chmod 755 /pistis-dq-orchestrator/run.sh

CMD ["/pistis-dq-orchestrator/run.sh"]