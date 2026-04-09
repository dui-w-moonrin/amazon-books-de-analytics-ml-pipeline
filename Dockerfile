FROM apache/airflow:3.2.0

ADD requirements.txt /requirements.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt