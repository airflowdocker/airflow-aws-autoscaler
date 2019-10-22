FROM python:3.7.4

WORKDIR /opt/airflow-docker/airflow-aws-autoscaler/

COPY deps/ deps/

RUN pip install -r deps/requirements.in

COPY . ./

RUN ln -s /opt/airflow-docker/airflow-aws-autoscaler/scripts/autoscale.py /usr/local/bin/airflow-docker-aws-autoscale \
    && chmod +x /usr/local/bin/airflow-docker-aws-autoscale
