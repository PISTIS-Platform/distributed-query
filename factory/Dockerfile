FROM python:3.11

ENV APP_TOKEN=""
ENV LSH_SIZE=""
ENV LSH_THRESHOLD=""
ENV DB_HOST=""
ENV DB_PORT=""
ENV IAM_URL=""
ENV IAM_REALM=""
ENV IAM_PK=""
ENV IAM_AUDIENCE=""
ENV JWT_LOCAL="true"
ENV DS_URL=""

WORKDIR /pistis-dq-lsh
COPY . /pistis-dq-lsh

RUN pip install -r requirements.txt

RUN chmod 755 /pistis-dq-lsh/run.sh

CMD ["/pistis-dq-lsh/run.sh"]