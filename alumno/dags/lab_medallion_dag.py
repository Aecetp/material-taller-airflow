"""
===========================================================================
 LAB AUTOGUIADO: Pipeline Batch con Arquitectura Medallion en Apache Airflow
===========================================================================

 CONTEXTO:
 Este DAG implementa un pipeline de datos con 3 capas (Bronze → Silver → Gold),
 siguiendo el patrón Lakehouse Medallion. Tú completarás las piezas faltantes.

 ESTRUCTURA DEL PIPELINE:
   [Source CSV] → (Bronze: datos crudos) → (Silver: datos limpios) → (Gold: datos agregados)

 LO QUE YA ESTÁ HECHO:
   - La función transform_to_silver() está completa (pero tiene un fallo intencional).
   - La definición del DAG y la tarea transform_task ya existen.

 LO QUE TÚ DEBES HACER:
   Paso 1: Completar la función extract_to_bronze()
   Paso 2: Completar la función load_to_gold()
   Paso 3: Definir las 3 tareas como PythonOperator
   Paso 4: Establecer las dependencias entre tareas
   Paso 5: Ejecutar el DAG, analizar el fallo simulado en los logs
   Paso 6: Corregir el fallo y hacer Clear/Re-proceso en la UI de Airflow

 CONCEPTO CLAVE - IDEMPOTENCIA:
   Cada tarea sobrescribe su archivo de salida. Esto significa que puedes
   re-ejecutar el pipeline cuantas veces quieras sin generar datos duplicados.
   Eso es "idempotencia" en pipelines batch.
===========================================================================
"""

import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Configuración de rutas ─────────────────────────────────────────────────
# Detectamos la ruta base dinámicamente para que funcione
# tanto en la carpeta alumno/ como en solucionario/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')


# =============================================================================
# PASO 1: Completa esta función
# =============================================================================
def extract_to_bronze():
    """
    CAPA BRONZE (Datos Crudos)
    --------------------------
    Objetivo: Leer el archivo CSV original y guardarlo como JSON en la capa bronze.
    Esto simula la ingesta de datos crudos sin transformación.

    Instrucciones:
      1. Usa pd.read_csv() para leer el archivo desde source_path
      2. Crea el directorio de destino con os.makedirs()
      3. Guarda el DataFrame como JSON con df.to_json(bronze_path, orient='records')

    Pistas:
      - pd.read_csv(source_path)  → lee un CSV y devuelve un DataFrame
      - os.makedirs(os.path.dirname(bronze_path), exist_ok=True) → crea la carpeta si no existe
      - df.to_json(path, orient='records') → guarda el DataFrame como una lista de objetos JSON
    """
    source_path = os.path.join(DATA_DIR, 'source', 'transactions.csv')
    bronze_path = os.path.join(DATA_DIR, 'bronze', 'transactions_raw.json')

    # --- TU CÓDIGO AQUÍ (3 líneas aprox.) ---
    # 1. df = pd.read_csv(...)
    # 2. os.makedirs(...)
    # 3. df.to_json(...)
    # -----------------------------------------

    print("⚠️ extract_to_bronze: Tarea incompleta. Escribe tu código arriba.")


# =============================================================================
# PASO 2 (YA RESUELTO - PERO CON FALLO SIMULADO):
# Lee esta función para entender qué hace. NO la modifiques todavía.
# Primero ejecuta el DAG y observa cómo falla. Luego vuelve aquí.
# =============================================================================
def transform_to_silver():
    """
    CAPA SILVER (Datos Limpios)
    ---------------------------
    Objetivo: Leer los datos de bronze, limpiar registros inválidos
    (montos negativos) y guardar en formato Parquet optimizado.
    """
    bronze_path = os.path.join(DATA_DIR, 'bronze', 'transactions_raw.json')
    silver_path = os.path.join(DATA_DIR, 'silver', 'transactions_clean.parquet')

    # ╔══════════════════════════════════════════════════════════════════════╗
    # ║  SIMULACIÓN DE FALLO EN PRODUCCIÓN                                  ║
    # ║  Este parámetro simula un error que ocurriría en un entorno real.    ║
    # ║  Después de ejecutar el DAG y ver el error en los Logs de Airflow,  ║
    # ║  cambia el valor a False y haz "Clear" a la tarea desde la UI.      ║
    # ╚══════════════════════════════════════════════════════════════════════╝
    SIMULAR_FALLO = True

    if SIMULAR_FALLO:
        raise ValueError(
            "ERROR SIMULADO: Fallo en la validación de esquema de la capa Silver. "
            "Para corregirlo, abre el archivo lab_medallion_dag.py, "
            "busca la variable SIMULAR_FALLO y cámbiala a False. "
            "Luego ve a la UI de Airflow y haz 'Clear' en esta tarea."
        )

    df = pd.read_json(bronze_path)

    # Limpieza: filtramos registros con montos negativos (datos inválidos)
    df_clean = df[df['amount'] >= 0]
    print(f"  → Registros originales: {len(df)}, Registros válidos: {len(df_clean)}")

    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    df_clean.to_parquet(silver_path, index=False)
    print(f"  → Datos limpios guardados en: {silver_path}")


# =============================================================================
# PASO 3: Completa esta función
# =============================================================================
def load_to_gold():
    """
    CAPA GOLD (Datos Agregados / Listos para Negocio)
    --------------------------------------------------
    Objetivo: Leer los datos limpios de silver, agrupar por fecha,
    sumar los montos de venta y guardar como reporte final.

    Instrucciones:
      1. Usa pd.read_parquet() para leer desde silver_path
      2. Agrupa por la columna 'date' y suma la columna 'amount'
      3. Crea el directorio de destino
      4. Guarda el resultado como Parquet en gold_path

    Pistas:
      - pd.read_parquet(silver_path) → lee un archivo Parquet
      - df.groupby('date', as_index=False)['amount'].sum() → agrupa y suma
      - df_agg.rename(columns={'amount': 'total_sales'}) → renombra la columna
      - df_agg.to_parquet(gold_path, index=False) → guarda como Parquet
    """
    silver_path = os.path.join(DATA_DIR, 'silver', 'transactions_clean.parquet')
    gold_path = os.path.join(DATA_DIR, 'gold', 'daily_sales.parquet')

    # --- TU CÓDIGO AQUÍ (5 líneas aprox.) ---
    # 1. df = pd.read_parquet(...)
    # 2. df_agregado = df.groupby(...)['amount'].sum()
    # 3. df_agregado = df_agregado.rename(...)
    # 4. os.makedirs(...)
    # 5. df_agregado.to_parquet(...)
    # -----------------------------------------

    print("⚠️ load_to_gold: Tarea incompleta. Escribe tu código arriba.")


# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
with DAG(
    dag_id='lab_medallion_batch_pipeline',
    description='Pipeline batch con arquitectura Medallion (Bronze → Silver → Gold)',
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,   # Ejecución manual (sin programación automática)
    catchup=False,            # No ejecutar fechas pasadas
    tags=['curso', 'batch', 'medallion']
) as dag:

    # =========================================================================
    # PASO 4: Define las 3 tareas usando PythonOperator
    # =========================================================================
    # Ejemplo de cómo se define una tarea:
    #
    #   mi_tarea = PythonOperator(
    #       task_id='nombre_unico_de_tarea',
    #       python_callable=nombre_de_la_funcion
    #   )
    #
    # --- TU CÓDIGO AQUÍ: Define extract_task, transform_task y load_task ---

    # Esta tarea ya está definida para que el DAG cargue y puedas ver el fallo:
    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver
    )

    # TODO: Define extract_task (task_id='extract_to_bronze', callable=extract_to_bronze)
    # TODO: Define load_task (task_id='load_to_gold', callable=load_to_gold)

    # =========================================================================
    # PASO 5: Establece el orden de ejecución (dependencias)
    # =========================================================================
    # En Airflow, el operador >> indica "va después de".
    # Ejemplo: tarea_a >> tarea_b >> tarea_c
    #
    # TODO: Escribe la línea de dependencias aquí:
    # extract_task >> transform_task >> load_task
