from fastapi import FastAPI, HTTPException
import duckdb
import asyncpg
import yaml
import os
from fsspec import filesystem

app = FastAPI()

# Load configuration
config_path = os.getenv('CONFIG_PATH', 'config.yaml')
with open(config_path, 'r') as stream:
    config = yaml.safe_load(stream)

# Register Storj as an S3-compatible filesystem
duckdb.register_filesystem(filesystem('s3'))

@app.post("/etl")
async def etl():
    try:
        # Set S3 credentials
        duckdb.sql(f"SET s3_endpoint='{config['storj']['endpoint']}'")
        duckdb.sql(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}'")
        duckdb.sql(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}'")

        # Query and aggregate using DuckDB
        df = duckdb.sql(f'''
            SELECT distance, SUM(air_time) as sum_air_time
            FROM read_parquet('s3://{config['storj']['bucket_name']}/{config['storj']['parquet_file_path']}')
            GROUP BY distance
        ''').fetchdf()

        # Load data into Postgres
        pg_conn = await asyncpg.connect(
            host=config['postgres']['host'],
            port=config['postgres']['port'],
            user=config['postgres']['user'],
            password=config['postgres']['password'],
            database=config['postgres']['dbname']
        )

        async with pg_conn.transaction():
            for index, row in df.iterrows():
                await pg_conn.execute(
                    f"INSERT INTO {config['target_table']} (distance, sum_air_time) VALUES ($1, $2)",
                    row['distance'], row['sum_air_time']
                )
        
        await pg_conn.close()
        
        return {"message": "ETL Process Completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
