#!/bin/bash

/opt/airflow/start-services.sh
/opt/airflow/start.sh
airflow users create --email ghent.zh@gmail.com --firstname Gent --lastname Zhubi --password admin --role Admin --username admin
bash set_connections_and_variables.sh
airflow users list
