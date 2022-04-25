FROM apache/airflow:2.0.0-python3.8
RUN pip3 install psycopg2-binary
RUN pip3 install apache-airflow-providers-postgres 
RUN pip3 install pycoingecko 
RUN pip3 install pymongo
ENV AIRFLOW_HOME=/opt/airflow
RUN mkdir /opt/airflow/packages
COPY ./packages.pth /usr/local/lib/python3.8/site-packages
