import logging
import time
import os
import psutil
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from cryptography.fernet import Fernet

# -------------------------------------------------------------------------
# ORTAK AIRFLOW VARIABLE TABANLI ANAHTAR YÖNETİMİ
# -------------------------------------------------------------------------
try:
    ENCRYPTION_KEY = Variable.get("tez_encryption_key")
except Exception:
    ENCRYPTION_KEY = Fernet.generate_key().decode('utf-8')
    Variable.set("tez_encryption_key", ENCRYPTION_KEY)

cipher_suite = Fernet(ENCRYPTION_KEY.encode('utf-8'))

def encrypt_value(value):
    if not value: 
        return value
    return cipher_suite.encrypt(str(value).encode('utf-8')).decode('utf-8')

def transfer_encrypted_dynamic(**kwargs):
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / (1024 * 1024)
    start_time = time.time()
        
    dag_run = kwargs.get('dag_run')
    batch_size = 10000
    if dag_run and dag_run.conf and 'batch_size' in dag_run.conf:
        batch_size = dag_run.conf['batch_size']
    
    logging.info(f"Dinamik batch_size kullanılıyor: {batch_size}")

    try:
        mongo_hook = MongoHook(conn_id='mongo_default')
        mongo_client = mongo_hook.get_conn()
        
        # 1. MongoDB Koleksiyon Kontrolü ve Esnek İsimlendirme
        db = mongo_client["tez_source_db"]
        existing_collections = db.list_collection_names()
        logging.info(f"MongoDB 'tez_source_db' içindeki mevcut koleksiyonlar: {existing_collections}")

        # Koleksiyon adını belirle (customers_50m_raw, customers_50M_raw veya customers_raw)
        target_collection_name = "customers_50m_raw"
        if target_collection_name not in existing_collections:
            if "customers_50M_raw" in existing_collections:
                target_collection_name = "customers_50M_raw"
            elif "customers_raw" in existing_collections:
                target_collection_name = "customers_raw"
        
        logging.info(f"Hedef Alınan MongoDB Koleksiyonu: {target_collection_name}")
        mongo_collection = db[target_collection_name]
        
        # Koleksiyondaki toplam doküman sayısını logla
        total_docs_in_mongo = mongo_collection.count_documents({})
        logging.info(f"MongoDB Koleksiyonundaki Toplam Doküman Sayısı: {total_docs_in_mongo}")

        if total_docs_in_mongo == 0:
            logging.warning("⚠️ UYARI: Hedef MongoDB koleksiyonu tamamen BOŞ! Aktarım yapılmayacak.")

        postgres_hook = PostgresHook(postgres_conn_id='postgres_company_db')
        pg_conn = postgres_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        
        # 2. PostgreSQL Performans Ayarları
        logging.info("PostgreSQL oturum parametreleri ayarlanıyor ve CHECKPOINT çalıştırılıyor...")
        pg_cursor.execute("SET synchronous_commit = off;")
        pg_cursor.execute("SET work_mem = '512MB';")
        pg_cursor.execute("CHECKPOINT;")
        pg_conn.commit()

        # 3. Şema ve Tablo Kontrolü
        logging.info("masked_50m şeması ve encrypted_50m_customers tablosu kontrol ediliyor/oluşturuluyor...")
        pg_cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS masked_50m;
            
            CREATE TABLE IF NOT EXISTS masked_50m.encrypted_50m_customers (
                mongo_id VARCHAR(50),
                musteri_id VARCHAR(100),
                ad_soyad VARCHAR(255),
                tckn TEXT,
                email VARCHAR(255),
                telefon VARCHAR(100),
                kredi_karti TEXT,
                bakiye NUMERIC(12, 2),
                kayit_tarihi TIMESTAMP
            );
        """)
        pg_conn.commit()

        logging.info("encrypted_50m_customers tablosu temizleniyor...")
        pg_cursor.execute("TRUNCATE TABLE masked_50m.encrypted_50m_customers;")
        pg_conn.commit()

        # 4. 50M Optimizasyonu: batch_size ile okuma
        mongo_cursor = mongo_collection.find(batch_size=batch_size)
        batch_data = []
        total_inserted = 0

        for doc in mongo_cursor:
            encrypted_record = (
                str(doc.get('_id', '')),
                str(doc.get('musteri_id', '')),
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
                    INSERT INTO masked_50m.encrypted_50m_customers 
                    (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                    VALUES %s
                """
                execute_values(pg_cursor, insert_query, batch_data)
                pg_conn.commit()
                total_inserted += len(batch_data)
                batch_data.clear()

        # Kalan son veriler
        if batch_data:
            insert_query = """
                INSERT INTO masked_50m.encrypted_50m_customers 
                (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                VALUES %s
            """
            execute_values(pg_cursor, insert_query, batch_data)
            pg_conn.commit()
            total_inserted += len(batch_data)

        pg_cursor.close()
        pg_conn.close()
                
        # Raporlama
        end_time = time.time()
        end_mem = process.memory_info().rss / (1024 * 1024)
                
        duration = end_time - start_time
        throughput = total_inserted / duration if duration > 0 else 0
        mem_used_mb = end_mem - start_mem

        logging.info("==================================================")
        logging.info("📊 PERFORMANS RAPORU (FERNET ŞİFRELEME - 50M)")
        logging.info("==================================================")
        logging.info(f"Toplam Aktarılan Satır : {total_inserted}")
        logging.info(f"Kullanılan Batch Size  : {batch_size}")
        logging.info(f"Toplam Geçen Süre      : {duration:.2f} saniye")
        logging.info(f"Aktarım Hızı           : {throughput:.2f} satır/saniye")
        logging.info(f"Bellek Tüketimi (Artış): {mem_used_mb:.2f} MB")
        logging.info("==================================================")

    except Exception as e:
        logging.error(f"50m Şifrelenmiş aktarım başarısız: {str(e)}")
        raise

default_args = {
    'owner': 'salih',
    'start_date': datetime(2026, 4, 25),
}

with DAG(
    dag_id='mongo_to_postgres_ENCRYPTED_50M',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tez', 'encrypted', 'fernet', '50M']
) as dag:

    transfer_task = PythonOperator(
        task_id='run_encrypted_metrics_50m',
        python_callable=transfer_encrypted_dynamic,
        provide_context=True
    )