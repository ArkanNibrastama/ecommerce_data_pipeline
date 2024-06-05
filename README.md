# E-Commerce Data Pipeline

## Overview
The E-Commerce Data Pipeline project aims to streamline the process of gathering order data from Shopify API, processing it, and loading it into a data warehouse for further analysis and visualization. By automating this pipeline, businesses can gain valuable insights into their sales performance, customer behavior, and inventory management, enabling data-driven decision-making to enhance their e-commerce operations.

![architecture](./img/architecture.png)

## Features
- Data Extraction: The pipeline fetches order data in real-time from the Shopify API, ensuring that the most up-to-date information is captured.
- Data Transformation: Upon retrieval, the raw data undergoes transformation processes such as cleaning, normalization, and enrichment to ensure consistency and accuracy.
- Data Loading: Processed data is loaded into a data warehouse, where it is organized and stored efficiently for easy access and analysis.
- Automated Workflow: The pipeline is designed to run automatically at scheduled intervals, reducing manual intervention and ensuring data freshness.
- Scalability: The architecture of the pipeline is scalable, allowing it to handle large volumes of data as the business grows.
- Customizable Analysis: Data stored in the warehouse can be analyzed using various BI tools and techniques to derive actionable insights tailored to the specific needs of the e-commerce store.
- Visualization: Insights gained from the analysis can be visualized through dashboards and reports, providing stakeholders with intuitive representations of key metrics and trends.

## Benefits
✅ <b>Actionable Insights</b><br>
Enables businesses to make informed decisions based on data-driven insights.

✅ <b>Efficiency</b><br>Automates the data pipeline, saving time and resources required for manual data handling.

✅ <b>Scalability</b><br>Accommodates the growing data needs of the e-commerce business.

✅ <b>Competitive Advantage</b><br>Harnesses the power of data to stay ahead in a competitive market landscape.

## Project Briefing
This project utilizes Google Compute Engine as the platform for the data pipeline.

## Set up
1. Install Git

    ```bash
    sudo apt-get update
    sudo apt-get install git-all
    ```
    make sure if the git has installed
    ```bash
    sudo git version
    ```

2. Install Docker

    You can find the installation [here](https://docs.docker.com/engine/install/ubuntu/).

    
    and make sure the docker-compose has installed
    ```bash
    sudo apt-get update
    sudo apt-get install docker-compose-plugin
    ```

3. Clone repository

    ```bash
    sudo git clone https://github.com/ArkanNibrastama/ecommerce_data_pipeline.git
    ```

4. Setup the service_acc_key.json & creds.py with your own key
    ```json
    //service_acc_key.json
    {
        "SERVICE_ACC_KEY" : "YOUR SERVICE ACCOUNT JSON FILE"    
    }
  
    ```
    ```python
    #/dags/creds.py
    url = "{YOUR SHOPIFY STORE URL}"
    api_version = "{VERSION OF SHOPIFY API}"
    token = "{YOUR SHOPIFY API TOKEN}"
    ```

5. Build the docker images for Spark and Airflow

    ```bash
    sudo docker build -f Dockerfile.Spark . -t spark
    sudo docker build -f Dockerfile.Airflow . -t airflow-spark
    ```

6. Make a directory called 'logs' to store log from Airflow
    ```bash
    sudo mkdir logs
    ```
    and make sure the logs dierctory is accessable
    ```bash
    sudo chmod -R 777 logs/
    ```

7. Build all the containers

    ```bash
    sudo docker-compose up -d
    ```

8. Set up Airflow connection
    
    - Access *{YOUR EXTERNAL IP}:9090* to access Spark UI
    - Access *{YOUR EXTERNAL IP}:8080* to access Airflow UI

    set up spark_default connection with your spark master url

    ![spark_default connection](./img/spark_conn.png)

    and set up for GCP connection

    ![gcp connection](./img/gcp_con.png)

## Conclusion
The E-Commerce Data Pipeline project offers a robust solution for managing and analyzing order data from Shopify, empowering e-commerce businesses to optimize their operations and drive growth through data-driven decision-making.