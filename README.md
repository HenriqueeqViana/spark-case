# Spark ETL Project with Docker

This project sets up a **Spark** standalone cluster using **Docker** and performs ETL operations on CSV files, leveraging **MinIO** for storage and **Dataflint** for data quality monitoring. The project uses **PySpark**, **Ganglia**, and **Spark UI** for analyzing and monitoring Spark jobs.

## Project Structure

- `app/` - Contains all the Python scripts:
  - `main.py` - Orchestrates the execution of the pipeline.
  - `extract.py` - Handles data extraction from CSV files.
  - `transform.py` - Handles data transformations.
  - `load.py` - Loads the transformed data into Delta Lake (MinIO).
  - `analyse.py` - Contains the queries to answer the analysis questions.
  - `utils.py` - Contains utility functions and UDFs (e.g., for calculating business days).
  
- `Dockerfile` - Sets up the Spark container.
- `docker-compose.yml` - Orchestrates the services (Spark, Ganglia, MinIO, Dataflint).
- `spark-defaults.conf` - Configures Spark settings and integrations (MinIO, Dataflint).
- `.gitignore` - Git ignore file to avoid committing unnecessary files.

## Prerequisites

Make sure you have the following tools installed on your machine:
- **Docker** - For containerization.
- **Docker Compose** - To manage multi-container Docker applications.
- **Resource in local machine** - To running containers

## Setup

1. Clone this repository:
    ```bash
    git clone https://github.com/yourusername/spark-etl-docker.git
    cd spark-etl-docker
    ```

2. Build and start the Docker containers using Docker Compose:
    ```bash
    docker-compose up --build
    ```

3. The following services will be started:
    - **Spark Master** and **Spark Worker** (for Spark jobs).
    - **Ganglia** (for monitoring the Spark jobs' performance).
    - **MinIO** (for object storage, similar to AWS S3).
    - **Dataflint** (for monitoring data quality).

4. Once the services are up, you can start the ETL process by executing the `main.py` file. Open a new terminal and run:
    ```bash
    docker exec -it spark-master bash
    cd /app
    python main.py
    ```

5. After running the ETL pipeline, you can access the **Spark UI** by navigating to `http://localhost:8080` and **Ganglia** at `http://localhost:8081` in your browser.

6. To check **Dataflint**, open your browser and visit `http://localhost:8888`.

## Configuration

- **MinIO**:
  - Endpoint: `http://localhost:9000`
  - Access Key: `minio_access_key`
  - Secret Key: `minio_secret_key`

- **Spark UI**:
  - Access the Spark UI at `http://localhost:8080`.

- **Ganglia**:
  - Access Ganglia at `http://localhost:8081`.

- **Dataflint**:
  - Access Dataflint at `http://localhost:8080`.

## Data Quality Monitoring

Dataflint is integrated into the Spark job. It will monitor the quality of the data processed through the pipeline. You can access the **Dataflint UI** at `http://localhost:8888` to see the data quality status and configure alerts.

## Running Queries

Once the ETL process is completed, you can view the results directly in Spark. For example, to get the highest revenue by color for each year or the average lead time by product category, simply check the logs of the `main.py` run or access Spark’s **SQL** interface.

## Clean up

To stop the services, run:
```bash
docker-compose down
```
 
## Reference 
[Spark Documentation](https://spark.apache.org/docs/3.5.3/index.html)
[Setting up a Spark Standalone Cluster on Docker](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b)