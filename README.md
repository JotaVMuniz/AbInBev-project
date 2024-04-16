# AbInBev-project

![alt text](https://github.com/JotaVMuniz/AbInBev-project/blob/main/images/abinbev-app.drawio.png)

Spark Version : 3.5.1
Airflow Version : 2.9.0

This is an airflow-spark application with the aim of consuming data from the open Brewery DB api and transforming it according to the concepts of the medallion architecture. Please follow the steps below to run the application :)

## Airflow setup

Our application uses HTTP and Spark connections to receive and transform data. Please follow setup as it shows below:
![alt text](https://github.com/JotaVMuniz/AbInBev-project/blob/main/images/spark_config.png)

![alt text](https://github.com/JotaVMuniz/AbInBev-project/blob/main/images/http_config.png)

## Quick Start
For a quick start, run the following command to build your docker image:
> docker compose up -d --build

Is expected that an error may occur due it's the first time we are initializing airflow environment.
Solution: Restart the webserver service and it should work in the next executions! :)


## Error handling
Apache Airflow provides robust error handling and monitoring support to ensure the reliability and stability of workflows.
Logs are stored in a centralized metadata database and can be accessed through the Airflow UI or command-line interface.
You can also define the number of retries and your email alert in the default_args section located in *data_api_process.py*