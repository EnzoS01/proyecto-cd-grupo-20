# dags/energy_demand_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json
import os
import logging
import numpy as np
from pytz import timezone
import random
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 9),  # Empezar desde hoy
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

def get_data_dir():
    """
    Carpeta local mapeada para guardar CSV y JSON dentro del contenedor
    que será reflejada en Windows en C:/Users/dell/Desktop/proyecto-cd-grupo-20/data
    """
    data_dir = "/usr/local/airflow/data"  # Cambiado de /opt/airflow/data
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def download_cammesa_data(region_id, region_name, **kwargs):
    """
    Descarga datos históricos de CAMMESA para una región (últimos 247 días)
    usando el endpoint 'ObtieneDemandaYTemperaturaRegionByFecha'.
    """
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date

    # Fecha final = día de ejecución, fecha inicial = 60 días antes
    fecha_fin = execution_date.date()
    fecha_inicio = fecha_fin - timedelta(days=247)

    data_dir = get_data_dir()
    out_csv = f"{data_dir}/cammesa_data_{region_name}_{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin.strftime('%Y%m%d')}.csv"

    url = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegionByFecha"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://api.cammesa.com/",
    }

    all_dfs = []
    for single_date in pd.date_range(fecha_inicio, fecha_fin, freq="D"):
        fecha_str = single_date.strftime("%Y-%m-%d")
        params = {"id_region": str(region_id), "fecha": fecha_str}
        
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60, verify=False)
            r.raise_for_status()
            data_json = r.json()

            if isinstance(data_json, list) and len(data_json) > 0:
                df = pd.DataFrame(data_json)
                df["fecha_consulta"] = fecha_str  # registrar de qué fecha vino
                all_dfs.append(df)
                logging.info(f"[{region_name}] Datos obtenidos para {fecha_str} ({len(df)} registros)")
            else:
                logging.warning(f"[{region_name}] Sin datos para {fecha_str}")

        except Exception as e:
            logging.error(f"[{region_name}] Error en fecha {fecha_str}: {e}")

        # Esperar entre 2 y 5 segundos antes del próximo request
        sleep_time = random.uniform(2, 5)
        logging.info(f"[{region_name}] Esperando {sleep_time:.2f} segundos antes de la próxima consulta...")
        time.sleep(sleep_time)

    if not all_dfs:
        raise ValueError(f"No se obtuvieron datos históricos para {region_name}")

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Limpieza de columnas (ej: 'Hoy')
    if "Hoy" in final_df.columns:
        final_df["Hoy"] = (
            final_df["Hoy"].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(" ", "", regex=False)
        )
        final_df["Hoy"] = pd.to_numeric(final_df["Hoy"], errors="coerce")

    final_df.to_csv(out_csv, index=False, sep=";", encoding="utf-8")
    logging.info(f"[{region_name}] Guardado histórico (60 días) en {out_csv}")

    ti.xcom_push(key=f'cammesa_file_path_{region_name}', value=out_csv)
    return f"Datos CAMMESA históricos {region_name}: {out_csv}"


def extract_weather_data(region_name, latitude, longitude, **kwargs):
    """
    Extrae datos climáticos históricos de Open-Meteo (últimos 247 días)
    para una región específica, usando la API de archive.
    """
    ti = kwargs['ti']
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date

    # Fechas de rango (últimos 60 días hasta la fecha de ejecución)
    fecha_fin = execution_date.date()
    fecha_inicio = fecha_fin - timedelta(days=247)

    argentina_tz = timezone('America/Argentina/Buenos_Aires')

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": fecha_inicio.strftime("%Y-%m-%d"),
        "end_date": fecha_fin.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,pressure_msl,cloudcover",
        "timezone": "auto"
    }

    logging.info(f"[{region_name}] Descargando datos climáticos históricos desde {params['start_date']} hasta {params['end_date']}")

    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Crear DataFrame con las variables horarias
        hourly = data.get("hourly", {})
        for key in ["time","temperature_2m","relative_humidity_2m","precipitation",
                    "wind_speed_10m","wind_direction_10m","pressure_msl","cloudcover"]:
            if key not in hourly:
                hourly[key] = [np.nan] * len(hourly.get("time", []))
        weather_df = pd.DataFrame(hourly)

        # Asegurar index temporal
        weather_df['time'] = pd.to_datetime(weather_df['time'])
        weather_df = weather_df.set_index('time')

        # Guardar JSON histórico por región
        data_dir = get_data_dir()
        weather_file_path = f"{data_dir}/weather_data_{region_name}_{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin.strftime('%Y%m%d')}.json"
        with open(weather_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # XComs (para merge posterior)
        ti.xcom_push(key=f"weather_data_{region_name}", value=hourly)
        ti.xcom_push(key=f"weather_file_path_{region_name}", value=weather_file_path)

        return f"[{region_name}] Datos climáticos históricos obtenidos: {weather_file_path}"

    except Exception as e:
        logging.error(f"[{region_name}] Error al obtener datos climáticos históricos: {str(e)}")
        raise



def process_and_merge_data(**kwargs):
    ti = kwargs["ti"]
    execution_date = kwargs.get('execution_date') or kwargs.get('data_interval_start') or ti.execution_date
    file_date_str = execution_date.strftime('%Y%m%d')

    argentina_tz = timezone('America/Argentina/Buenos_Aires')

    regions = [
        {"name": "edenor", "cammesa_task": "download_cammesa_edenor", "weather_key": "weather_data_edenor"},
        {"name": "edesur", "cammesa_task": "download_cammesa_edesur", "weather_key": "weather_data_edesur"},
        {"name": "edelap", "cammesa_task": "download_cammesa_edelap", "weather_key": "weather_data_edelap"},
    ]

    output_files = []

    for region in regions:
        region_name = region["name"]

        # --- Recuperar datos del XCom ---
        weather_data = ti.xcom_pull(task_ids=f"extract_weather_{region_name}", key=region["weather_key"])
        if not weather_data:
            raise ValueError(f"No se encontraron datos climáticos para {region_name} en XCom.")

        cammesa_file_path = ti.xcom_pull(task_ids=region["cammesa_task"], key=f"cammesa_file_path_{region_name}")
        if not cammesa_file_path:
            raise ValueError(f"No se encontró archivo CAMMESA para {region_name} en XCom.")

        # --- Procesar CAMMESA ---
        cammesa_df = pd.read_csv(cammesa_file_path, sep=';')
        cammesa_df.columns = cammesa_df.columns.str.strip()  # limpiar espacios invisibles

        if 'dem' not in cammesa_df.columns:
            raise KeyError(f"La columna 'dem' no existe en {cammesa_file_path}. Columnas: {cammesa_df.columns.tolist()}")

        # Normalizar valores de demanda
        cammesa_df['dem'] = (
            cammesa_df['dem'].astype(str)
            .str.replace(',', '.', regex=False)
            .str.replace(' ', '', regex=False)
        )
        cammesa_df['dem'] = pd.to_numeric(cammesa_df['dem'], errors='coerce')

        # Parseo de fechas CAMMESA
        cammesa_df["fecha_dt"] = pd.to_datetime(cammesa_df["fecha"], errors="coerce")
        cammesa_df = cammesa_df.dropna(subset=["fecha_dt"])
        cammesa_df = cammesa_df[cammesa_df["fecha_dt"].dt.minute == 0]  # solo en punto
        cammesa_df = cammesa_df.sort_values("fecha_dt")

        # Extraer hora en tz Argentina
        cammesa_df["hour_only"] = cammesa_df["fecha_dt"].dt.floor("H")
        if cammesa_df["hour_only"].dt.tz is None:
            cammesa_df["hour_only"] = cammesa_df["hour_only"].dt.tz_localize(argentina_tz)

        # --- Procesar Weather ---
        weather_df = pd.DataFrame(weather_data)
        weather_df["datetime_arg"] = pd.to_datetime(weather_df["time"], errors="coerce")
        weather_df["hour_only"] = weather_df["datetime_arg"].dt.floor("H")
        if weather_df["hour_only"].dt.tz is None:
            weather_df["hour_only"] = weather_df["hour_only"].dt.tz_localize(argentina_tz)
        else:
            weather_df["hour_only"] = weather_df["hour_only"].dt.tz_convert(argentina_tz)

        # --- Merge exacto por hora ---
        merged_df = pd.merge(
            cammesa_df[["hour_only", "dem", "fecha_dt"]],
            weather_df.drop(columns=["time", "datetime_arg"]),
            on="hour_only",
            how="inner"
        )

        if merged_df.empty:
            raise ValueError(f"No hubo coincidencia entre horas de CAMMESA y Weather para {region_name}")

        # Renombrar columnas
        merged_df = merged_df.rename(columns={"hour_only": "fecha", "fecha_dt": "fecha_original_cammesa"})

        # Reordenar columnas
        column_order = ["fecha", "fecha_original_cammesa", "dem"] + \
                       [c for c in merged_df.columns if c not in ["fecha", "fecha_original_cammesa", "dem"]]
        merged_df = merged_df[column_order]

        # --- Guardar CSV por región ---
        data_dir = get_data_dir()
        output_file = f"{data_dir}/energy_weather_dataset_{region_name}_{file_date_str}.csv"
        merged_df.to_csv(output_file, index=False, encoding="utf-8")
        logging.info(f"[{region_name}] Dataset final guardado en {output_file} con {len(merged_df)} registros")

        # XCom por región
        ti.xcom_push(key=f'final_dataset_path_{region_name}', value=output_file)
        output_files.append(output_file)

    return output_files



def save_to_master_dataset(**kwargs):
    """
    Agrega los datos procesados al dataset maestro histórico para cada región
    y guarda los 3 CSVs maestros en la carpeta mapeada al host.
    """
    import pandas as pd
    import os
    import logging

    ti = kwargs['ti']

    regions = ["edenor", "edesur", "edelap"]
    data_dir = get_data_dir()

    for region in regions:
        # Recuperar dataset diario procesado
        daily_dataset_path = ti.xcom_pull(key=f'final_dataset_path_{region}', task_ids='process_merge_data')
        if not daily_dataset_path:
            logging.warning(f"No se encontró dataset diario para {region}, se salta.")
            continue

        daily_df = pd.read_csv(daily_dataset_path)

        # Asegurar columna fecha como datetime
        if 'fecha' not in daily_df.columns:
            raise ValueError(f"No se encontró la columna 'fecha' en daily_df de {region}")
        daily_df['fecha'] = pd.to_datetime(daily_df['fecha'], errors='coerce')

        # Archivo maestro por región
        master_file_path = os.path.join(data_dir, f"master_energy_dataset_{region}.csv")

        if os.path.exists(master_file_path):
            master_df = pd.read_csv(master_file_path)
            
            # CORRECCIÓN: Siempre convertir la columna fecha a datetime
            if 'fecha' in master_df.columns:
                master_df['fecha'] = pd.to_datetime(master_df['fecha'], errors='coerce')
            else:
                raise ValueError(f"No se encontró la columna 'fecha' en master_df de {region}")

            # Combinar, eliminar duplicados y ordenar
            combined_df = pd.concat([master_df, daily_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['fecha'], inplace=True)
            combined_df.sort_values('fecha', inplace=True)
            combined_df.to_csv(master_file_path, index=False)
            logging.info(f"[{region}] Dataset maestro actualizado: {len(combined_df)} registros")
        else:
            daily_df.sort_values('fecha', inplace=True)
            daily_df.to_csv(master_file_path, index=False)
            logging.info(f"[{region}] Dataset maestro creado: {len(daily_df)} registros")

    return f"Datasets maestros actualizados en {data_dir}"


# DAG principal
with DAG(
    'energy_demand_pipeline',
    default_args=default_args,
    description='Pipeline completo para datos de demanda energética y clima',
    schedule=None,  # Sin programación automática por ahora
    catchup=False,
    max_active_runs=1,
    tags=['cammesa', 'energy', 'weather', 'ml']
) as dag:
    
    # Descargar datos de CAMMESA
    download_edenor = PythonOperator(
        task_id="download_cammesa_edenor",
        python_callable=download_cammesa_data,
        op_args=[1077, "edenor"],
    )

    download_edesur = PythonOperator(   
        task_id="download_cammesa_edesur",
        python_callable=download_cammesa_data,
        op_args=[1078, "edesur"],
    )

    download_edelap = PythonOperator(
        task_id="download_cammesa_edelap",
        python_callable=download_cammesa_data,
        op_args=[1943, "edelap"],
    )
    
    # Extraer datos climáticos
    extract_weather_edenor = PythonOperator(
        task_id='extract_weather_edenor',
        python_callable=extract_weather_data,
        op_args=["edenor", -34.57, -58.46]  # ejemplo de coordenadas
    )

    extract_weather_edesur = PythonOperator(
        task_id='extract_weather_edesur',
        python_callable=extract_weather_data,
        op_args=["edesur", -34.66, -58.45]
    )

    extract_weather_edelap = PythonOperator(
        task_id='extract_weather_edelap',
        python_callable=extract_weather_data,
        op_args=["edelap", -34.91, -57.95]
    )
    
    # Procesar y combinar datos
    process_merge_task = PythonOperator(
        task_id='process_merge_data',
        python_callable=process_and_merge_data,
        execution_timeout=timedelta(minutes=10)
    )
    
    # Guardar en dataset maestro
    save_master_task = PythonOperator(
        task_id='save_master_dataset',
        python_callable=save_to_master_dataset,
        execution_timeout=timedelta(minutes=5)
    )
    
    # Definir dependencias
[download_edenor, download_edesur, download_edelap,
 extract_weather_edenor, extract_weather_edesur, extract_weather_edelap] >> process_merge_task >> save_master_task

