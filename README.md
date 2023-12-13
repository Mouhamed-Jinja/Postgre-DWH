# Simple Data Integration Project
Not all solutions need to be complex. This project focuses on supporting small and medium-scale data scenarios, avoiding the need for technologies designed for massive datasets. It's a straightforward data integration project aiming to seamlessly combine data from various sources, including CSVs, JSON, Parquet (denormalized data), and a MySQL database.
### Architecture:
<img width="817" alt="post4" src="https://github.com/Mouhamed-Jinja/Postgre-DWH/assets/132110499/ba0263e5-52e1-45bc-9d1a-1b0d16fecd05">

## Project Overview
- Data Integration: The project involves integrating data from diverse sources into a unified format.
- Transformation with Spark: Utilizing Spark, the data is transformed to the desired format, preparing it for easy conversion into a Data Warehouse (DWH) star schema.
- PostgreSQL Data Warehousing: For the final destination load, PostgreSQL is employed as the data warehouse. This facilitates efficient storage and retrieval of the organized data.
- Infrastructure Management with Docker-Compose: Docker-compose is utilized to manage the project's services, ensuring seamless infrastructure orchestration.


## Target Audience
- This solution is meticulously designed to meet the requirements of a diverse data team, including:
-Business Intelligence (BI) Professionals: Leverage high-level, organized data to derive actionable insights and make data-driven decisions.

- Data Analysts: Dive into the PostgreSQL Data Warehouse for comprehensive data analysis and reporting.

- Scientists: Utilize the power of structured data to conduct in-depth research and scientific exploration.

- Machine Learning Engineers: Access integrated data for building and optimizing machine learning models.

### Seamless Interaction with PostgreSQL Data Warehouse
- Data team members can effortlessly interact with the PostgreSQL Data Warehouse through:

- Jupyter Services: Utilize the Jupyter services established in the Docker Compose file for interactive and collaborative data exploration.

- SQL Queries: Connect to the PostgreSQL database directly and write SQL queries to retrieve and analyze data according to specific needs.

- This ensures a flexible and user-friendly environment for a variety of data-related tasks, from exploratory data analysis to complex machine learning model development. The integration of Jupyter services adds an extra layer of accessibility, enabling team members to work in a familiar and interactive notebook environment.
### Star Schema:
<img width="644" alt="Screenshot 2023-12-13 112353" src="https://github.com/Mouhamed-Jinja/Postgre-DWH/assets/132110499/f12c22ff-4aeb-45af-a05f-bdc9e7004af4">

### Dashboard Created Using PowerBI:
- You can interact with the Dashboard through this link: https://www.novypro.com/project/fraud-sales-analysis
<img width="526" alt="Screenshot 2023-12-13 164544" src="https://github.com/Mouhamed-Jinja/Postgre-DWH/assets/132110499/e0782892-4af8-4357-a25b-1898e16f9109">

# Getting Started
To start using this solution, follow these simple steps:
- Clone the repo:
  ```
    https://github.com/Mouhamed-Jinja/Postgre-DWH.git
  ```
- Up the docker-compose file:
  ```
    docker-compose up -d
  ```
- Access Postgres Service and create a database, you can access it by inspecting the IP and the port and using any IDE to connect the Service:
  ```
    CREATE DATABASE fraud;
  ```
- Now The cluster is ready to submit the spark jobs, make sure that you are in the path where jars exist, or add the jars link before them:
  ```
    docker exec -it spark_master bash
  ```
  ```
    cd /drivers
  ```
  ```
    spark-submit --jars mysql-connector-j-8.2.0.jar,postgresql-42.5.3.jar /opt/bitnami/spark/jobs/Extract.py
  ```
  ```
    spark-submit --jars mysql-connector-j-8.2.0.jar,postgresql-42.5.3.jar /opt/bitnami/spark/jobs/Transform.py
  ```



