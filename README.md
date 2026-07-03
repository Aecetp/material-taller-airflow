# 🧪 Laboratorio Autoguiado: Apache Airflow — Pipelines Batch

> **Sesión 1 · Módulo: Data Architect** · Pipelines Batch y Orquestación

<!-- 
  👇 Reemplaza TU_USUARIO/TU_REPO con el nombre real de tu repositorio en GitHub
  Ejemplo: https://codespaces.new/datapath-academy/lab-airflow-batch
-->
[![Abrir en GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/TU_USUARIO/TU_REPO)

> 💡 **Tip para el profesor:** Reemplaza `TU_USUARIO/TU_REPO` en el link del badge de arriba con el usuario y nombre real del repositorio en GitHub antes de compartirlo con los alumnos.

---

## 📋 ¿Qué harás en este laboratorio?

Implementarás un **pipeline batch** con arquitectura **Lakehouse Medallion** (Bronze → Silver → Gold) usando Apache Airflow. Aprenderás de forma práctica:

- Cómo definir dependencias entre tareas en un DAG.
- Qué significa que un pipeline sea **idempotente**.
- Cómo leer **logs** de Airflow para diagnosticar un fallo.
- Cómo hacer un **re-procesamiento (Clear)** desde la interfaz gráfica.

---

## 🚀 Preparación del Entorno

### ▶️ Opción A: GitHub Codespaces (Recomendada — 1 clic)

1. Haz clic en el badge **"Abrir en GitHub Codespaces"** al inicio de este archivo.
2. Espera ~2 minutos mientras se instalan las dependencias automáticamente.
3. Cuando el entorno esté listo, abre una **terminal** en VS Code y ejecuta:
   ```bash
   export AIRFLOW_HOME=$(pwd)/alumno
   airflow standalone
   ```
4. Verás en la consola algo como:
   ```
   standalone | Airflow is ready
   standalone | Login with username: admin  password: XXXXXXXX
   ```
   **Copia la contraseña** — la necesitarás para entrar a la UI.

5. Codespaces te mostrará una notificación para abrir el **puerto 8080**. Haz clic en **"Open in Browser"** para acceder a Airflow.

### 🐳 Opción B: Docker Local (Respaldo)
Si no puedes usar Codespaces, tu profesor te proporcionará el paquete `entorno-docker.rar` con instrucciones para levantarlo localmente.

---

## 📝 Actividad Guiada: Paso a Paso

### Paso 1 — Explorar el DAG base
1. Abre el archivo `alumno/dags/lab_medallion_dag.py` en el editor.
2. Lee los comentarios del encabezado para entender la estructura.
3. Observa que hay **3 funciones** (una por capa del Medallion):
   - `extract_to_bronze()` → **Tú la completarás**
   - `transform_to_silver()` → Ya está hecha (pero tiene un fallo intencional 😈)
   - `load_to_gold()` → **Tú la completarás**

### Paso 2 — Completar la extracción (Bronze)
1. Busca la función `extract_to_bronze()` en el DAG.
2. Sigue las instrucciones en los comentarios para escribir ~3 líneas de código usando `pandas`.
3. **Concepto clave → Idempotencia**: La tarea sobrescribe su salida cada vez que corre. Puedes re-ejecutarla infinitas veces sin generar datos duplicados.

### Paso 3 — Completar la carga (Gold)
1. Busca la función `load_to_gold()` en el DAG.
2. Sigue las instrucciones para leer de Silver, agrupar por fecha y guardar el reporte.
3. **Concepto clave**: La capa Gold contiene datos **listos para negocio** — métricas, KPIs, reportes.

### Paso 4 — Definir las tareas y dependencias
1. Ve a la sección del DAG al final del archivo.
2. Define `extract_task` y `load_task` como `PythonOperator` (hay un ejemplo comentado).
3. Establece el orden: `extract_task >> transform_task >> load_task`.
4. **Concepto clave → Dependencias**: el operador `>>` garantiza que Silver no corra si Bronze falló.

### Paso 5 — Ejecutar y observar el fallo
1. Entra a la interfaz web de Airflow en el puerto 8080.
2. Busca el DAG `lab_medallion_batch_pipeline` en la lista.
3. Actívalo con el toggle (☁️ → ✅).
4. Haz clic en **"Trigger DAG"** (▶️) para lanzar una ejecución manual.
5. Observa el gráfico: la tarea `transform_to_silver` se pondrá en **rojo** ❌.

### Paso 6 — Leer los Logs y diagnosticar
1. Haz clic sobre la tarea roja `transform_to_silver`.
2. Selecciona **"Log"** en el popup.
3. Lee el mensaje de error. Verás:
   ```
   ValueError: ERROR SIMULADO: Fallo en la validación de esquema de la capa Silver...
   ```
4. **Pregunta**: ¿Qué variable en el código debes cambiar y a qué valor?

### Paso 7 — Corregir y Re-procesar (Clear)
1. Regresa al código y busca `SIMULAR_FALLO = True` en `transform_to_silver()`.
2. Cámbiala a `SIMULAR_FALLO = False`. Guarda el archivo.
3. En la UI de Airflow, haz clic en la tarea roja → **"Clear"** → marca **"Downstream"** → **"Confirm"**.
4. Observa cómo el pipeline completa exitosamente (todas las tareas en **verde** ✅).

---

## 🎯 Conceptos Cubiertos

| Concepto | Cómo se aplica en el lab |
|---|---|
| **Dependencias** | `extract >> transform >> load` — cada tarea espera a la anterior |
| **Idempotencia** | Cada tarea sobrescribe su salida. Re-ejecutar es seguro |
| **Reprocesos** | La función **Clear** permite re-ejecutar tareas fallidas sin perder el contexto |
| **Monitoreo** | Los **Logs** de Airflow muestran exactamente qué pasó y por qué |
| **Scheduling** | `schedule_interval=None` = manual; puede cambiarse a un cron como `@daily` |

---

## 📂 Estructura del Repositorio

```
.devcontainer/
└── devcontainer.json       ← Configura el entorno de Codespaces automáticamente

alumno/
├── dags/
│   └── lab_medallion_dag.py  ← ⭐ EL ARCHIVO QUE EDITARÁS
└── data/
    ├── source/
    │   └── transactions.csv  ← Dataset de entrada (40 transacciones simuladas)
    ├── bronze/               ← Se crea al ejecutar la tarea 1
    ├── silver/               ← Se crea al ejecutar la tarea 2
    └── gold/                 ← Se crea al ejecutar la tarea 3

solucionario/
└── dags/
    └── lab_medallion_dag.py  ← Solución completa (solo para el profesor)

requirements.txt              ← Dependencias: airflow, pandas, pyarrow
```

---

## ✅ Criterios de Éxito

- [ ] Las 3 funciones del DAG están implementadas.
- [ ] Las 3 tareas están definidas como `PythonOperator`.
- [ ] Las dependencias están establecidas con `>>`.
- [ ] Leíste los logs del fallo simulado e identificaste la causa.
- [ ] Corregiste el código y usaste **Clear** para re-procesar exitosamente.
- [ ] Todas las tareas terminaron en **verde** en la UI de Airflow. 🎉
