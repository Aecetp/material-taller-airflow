"""
===========================================================================
 SOLUCIONARIO: Pipeline Batch con Arquitectura Medallion en Apache Airflow
===========================================================================
 Este archivo contiene la solución completa del laboratorio.
 Úsalo como referencia o para ejecutar el pipeline sin completar los pasos.
===========================================================================
"""

import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')


def extract_to_bronze():
    """CAPA BRONZE: CSV → JSON crudo"""
    source_path = os.path.join(DATA_DIR, 'source', 'transactions.csv')
    bronze_path = os.path.join(DATA_DIR, 'bronze', 'transactions_raw.json')

    df = pd.read_csv(source_path)

    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
    df.to_json(bronze_path, orient='records')
    print(f"  → {len(df)} registros extraídos a bronze: {bronze_path}")


def transform_to_silver():
    """CAPA SILVER: JSON → Parquet limpio (sin montos negativos)"""
    bronze_path = os.path.join(DATA_DIR, 'bronze', 'transactions_raw.json')
    silver_path = os.path.join(DATA_DIR, 'silver', 'transactions_clean.parquet')

    # FALLO RESUELTO: En la versión del alumno esto es True
    SIMULAR_FALLO = False

    if SIMULAR_FALLO:
        raise ValueError("ERROR SIMULADO: Fallo en la validación de esquema de la capa Silver.")

    df = pd.read_json(bronze_path)

    df_clean = df[df['amount'] >= 0]
    print(f"  → Registros originales: {len(df)}, Registros válidos: {len(df_clean)}")

    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    df_clean.to_parquet(silver_path, index=False)
    print(f"  → Datos limpios guardados en: {silver_path}")


def load_to_gold():
    """CAPA GOLD: Parquet limpio → Parquet agregado (ventas diarias)"""
    silver_path = os.path.join(DATA_DIR, 'silver', 'transactions_clean.parquet')
    gold_path = os.path.join(DATA_DIR, 'gold', 'daily_sales.parquet')

    df = pd.read_parquet(silver_path)

    df_agregado = df.groupby('date', as_index=False)['amount'].sum()
    df_agregado = df_agregado.rename(columns={'amount': 'total_sales'})

    os.makedirs(os.path.dirname(gold_path), exist_ok=True)
    df_agregado.to_parquet(gold_path, index=False)
    print(f"  → Reporte de ventas diarias guardado en gold: {gold_path}")
    print(df_agregado.to_string(index=False))


with DAG(
    dag_id='lab_medallion_batch_pipeline',
    description='Pipeline batch con arquitectura Medallion (Bronze → Silver → Gold)',
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=['curso', 'batch', 'medallion']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_to_bronze',
        python_callable=extract_to_bronze
    )

    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver
    )

    load_task = PythonOperator(
        task_id='load_to_gold',
        python_callable=load_to_gold
    )

    extract_task >> transform_task >> load_task
