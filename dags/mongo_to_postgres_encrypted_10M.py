import logging
import time
import os
import psutil
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from cryptography.fernet import Fernet

# Şifreleme anahtarı
ENCRYPTION_KEY = Fernet.generate_key()
cipher_suite = Fernet(ENCRYPTION_KEY)

def encrypt_value(value):
    if not value: return value
    return cipher_suite.encrypt(str(value).encode('utf-8')).decode('utf-8')

def transfer_encrypted_dynamic(**kwargs):
    # Performans ölçümü başlangıcı
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / (1024 * 1024)
    start_time = time.time()
    
    # 1. Config'den batch_size al (yoksa 10000 kullan)
    dag_run = kwargs.get('dag_run')
    batch_size = 10000
    if dag_run and dag_run.conf and 'batch_size' in dag_run.conf:
        batch_size = dag_run.conf['batch_size']
        logging.info(f"Dinamik batch_size alındı: {batch_size}")
    
    try:
        mongo_hook = MongoHook(conn_id='mongo_default')
        mongo_client = mongo_hook.get_conn()
        mongo_collection = mongo_client["tez_source_db"]["customers_10M_raw"]
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_company_db')
        pg_conn = postgres_hook.get_conn()
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute("TRUNCATE TABLE masked_10m.encrypted_10m_customers;")
        pg_conn.commit()

        mongo_cursor = mongo_collection.find()
        batch_data = []
        total_inserted = 0

        for doc in mongo_cursor:
            encrypted_record = (
                str(doc.get('_id', '')),
                doc.get('musteri_id', ''),
                doc.get('ad_soyad', ''),
                encrypt_value(doc.get('tckn', '')),
                doc.get('email', ''),
                doc.get('telefon', ''),
                encrypt_value(doc.get('kredi_karti', '')),
                doc.get('bakiye', 0.0),
                doc.get('kayit_tarihi', None)
            )
            batch_data.append(encrypted_record)

            if len(batch_data) >= batch_size:
                insert_query = """
                    INSERT INTO masked_10m.encrypted_10m_customers 
                    (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                    VALUES %s
                """
                execute_values(pg_cursor, insert_query, batch_data)
                pg_conn.commit()
                total_inserted += len(batch_data)
                batch_data.clear()

        if batch_data:
            insert_query = """
                INSERT INTO masked_10m.encrypted_10m_customers 
                (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                VALUES %s
            """
            execute_values(pg_cursor, insert_query, batch_data)
            pg_conn.commit()
            total_inserted += len(batch_data)

        pg_cursor.close()
        pg_conn.close()
        
        # Metrik Hesaplamaları
        end_time = time.time()
        end_mem = process.memory_info().rss / (1024 * 1024)
        
        duration = end_time - start_time
        throughput = total_inserted / duration if duration > 0 else 0
        mem_used_mb = end_mem - start_mem

        # Tez İçin Detaylı Rapor
        logging.info("==================================================")
        logging.info("📊 TEZ PERFORMANS RAPORU (DİNAMİK ŞİFRELEME)")
        logging.info("==================================================")
        logging.info(f"Toplam Aktarılan Satır : {total_inserted}")
        logging.info(f"Kullanılan Batch Size  : {batch_size}")
        logging.info(f"Toplam Geçen Süre      : {duration:.2f} saniye")
        logging.info(f"Aktarım Hızı           : {throughput:.2f} satır/saniye")
        logging.info(f"Bellek Tüketimi (Artış): {mem_used_mb:.2f} MB")
        logging.info("==================================================")

    except Exception as e:
        logging.error(f"Hata: {str(e)}")
        raise

with DAG(
    dag_id='mongo_to_postgres_ENCRYPTED_10M',
    start_date=datetime(2026, 4, 25),
    schedule_interval=None,
    catchup=False
) as dag:

    transfer_task = PythonOperator(
        task_id='run_encrypted_metrics_10m',
        python_callable=transfer_encrypted_dynamic,
        provide_context=True
    )