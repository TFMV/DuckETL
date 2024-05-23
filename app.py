from fastapi import FastAPI, HTTPException
import duckdb
import asyncpg
import pandas as pd
import yaml
import os

app = FastAPI()

# Load configuration
config_path = os.getenv('CONFIG_PATH', 'config.yaml')
with open(config_path, 'r') as stream:
    config = yaml.safe_load(stream)

@app.post("/etl")
async def etl():
    try:
        # Register GCS filesystem with fsspec
        import fsspec
        from fsspec import filesystem
        fs = filesystem('gcs')
        duckdb.register_filesystem(fs)
        
        # Access Parquet file directly from GCS using DuckDB
        gcs_file_path = f"gcs://{config['gcs']['bucket_name']}/{config['gcs']['parquet_file_path']}"
        
        # Query and aggregate using DuckDB
        conn = duckdb.connect()
        df = conn.execute(f'''
            SELECT distance, SUM(air_time) as sum_air_time
            FROM read_parquet('{gcs_file_path}')
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
