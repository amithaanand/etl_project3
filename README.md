# etl_project3
Project Title: ETL Pipeline with Kafka, Spark, MongoDB Atlas Overview: This project implements an ETL (Extract, Transform, Load) pipeline that retrieves data from a random API, processes and cleans the data using Python scripts, utilizes Kafka for real-time data streaming, employs Apache Spark for data processing, and finally loads the processed data into MongoDB Atlas.

Repository Structure: │ ├── data_extraction/ │ ├── api_data_fetch.py │ └── data_cleaning.py │ ├── kafka_integration/ │ ├── kafka_producer.py │ └── kafka_consumer.py │ ├── spark_processing/ │ ├── spark_jobs.py │ └── spark_config/ │ └── spark_configurations.xml │ ├── mongodb_atlas_integration/ │ ├── mongodb_atlas_loader.py │ └── mongodb_atlas_queries/ │ └── query_scripts.js │ └── README.md

Components: Data Extraction: Python scripts fetch data from a random API endpoint.

Data Processing and Cleaning: Python scripts handle data transformation and cleaning tasks to prepare it for downstream processing.

Real-time Streaming with Kafka: Data is streamed in real-time using Apache Kafka, ensuring scalable and fault-tolerant data ingestion.

Data Processing with Apache Spark: Spark jobs are employed to perform complex data transformations, aggregations, and computations on the streaming data.

Data Storage with MongoDB Atlas: Processed data is stored persistently in MongoDB Atlas, providing a fully managed, cloud-based NoSQL database solution.

Technologies Used: Python: For data extraction, transformation, and cleaning. Apache Kafka: For real-time data streaming and ingestion. Apache Spark: For distributed data processing and analysis. MongoDB Atlas: For storing processed data in a cloud-hosted MongoDB database.

Environment File: The project includes MongoDB Cloud configuration details in the environment file for seamless integration.
