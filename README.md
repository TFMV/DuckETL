# DuckETL

DuckETL is a data pipeline project that demonstrates how to move data from a Parquet file stored on Google Cloud Storage (GCS) to a Postgres database for further analysis and visualization. The project utilizes DuckDB for querying and aggregating data directly from the Parquet file in GCS and FastAPI to manage the ETL process.

![DuckETL](assets/duck.webp)

## Project Structure

- `app.py`: The main application file for FastAPI.
- `config.yaml`: Configuration file for database and GCS details.
- `Dockerfile`: Docker configuration for building the application.
- `docker-compose.yml`: Docker Compose configuration for running the application.
- `requirements.txt`: Python dependencies.

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Google Cloud Storage bucket with the Parquet file
- Postgres database

### Setup

1. **Clone the Repository**

    ```sh
    git clone https://github.com/yourusername/DuckETL.git
    cd DuckETL
    ```

2. **Configure `config.yaml`**

    Update the `config.yaml` file with your GCS and Postgres details:

    ```yaml
    postgres:
      host: localhost
      port: 5432
      user: postgres
      password: mysecretpassword
      dbname: yourdbname

    gcs:
      service_account_key: /path/to/your/sa.json
      bucket_name: your-bucket-name
      parquet_file_path: path/to/your/file.parquet

    target_table: your_target_table
    ```

3. **Build and Run the Docker Container**

    ```sh
    docker-compose up --build
    ```

## Usage

The application exposes an endpoint `/etl` that triggers the ETL process. You can use tools like Postman to send a POST request to this endpoint.

### Example Request

```sh
POST /etl

{
  "message": "ETL Process Completed"
}
```

## Networking

Ensure that your local Postgres instance is accessible from within the Docker container. You may need to configure your network settings to allow connections.

## License
This project is licensed under the MIT License. See the LICENSE file for details.