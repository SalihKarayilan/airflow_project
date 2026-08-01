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

def transfer_baseline_unmasked():
    # 1. Performans Takibi Başlangıç
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / (1024 * 1024)
    start_time = time.time()
    
    batch_size = 10000
    
    try:
        logging.info("Baseline 50M aktarımı için bağlantılar kuruluyor...")
        
        mongo_hook = MongoHook(conn_id='mongo_default')
        mongo_client = mongo_hook.get_conn()
        mongo_collection = mongo_client["tez_source_db"]["customers_50M_raw"]
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_company_db')
        pg_conn = postgres_hook.get_conn()
        pg_cursor = pg_conn.cursor()

        # 2. 50M Şemayı ve Tabloyu Otomatik Oluştur (Yoksa)
        logging.info("masked_50m şeması ve unmasked_50m_customers tablosu kontrol ediliyor/oluşturuluyor...")
        pg_cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS masked_50m;
            
            CREATE TABLE IF NOT EXISTS masked_50m.unmasked_50m_customers (
                mongo_id VARCHAR(50),
                musteri_id VARCHAR(100),
                ad_soyad VARCHAR(255),
                tckn VARCHAR(50),
                email VARCHAR(255),
                telefon VARCHAR(100),
                kredi_karti VARCHAR(100),
                bakiye NUMERIC(12, 2),
                kayit_tarihi TIMESTAMP
            );
        """)
        pg_conn.commit()

        # 3. Var olan eski verileri temizle
        logging.info("unmasked_50m_customers tablosu temizleniyor...")
        pg_cursor.execute("TRUNCATE TABLE masked_50m.unmasked_50m_customers;")
        pg_conn.commit()

        mongo_cursor = mongo_collection.find()
        
        batch_data = []
        total_inserted = 0

        # 4. Veri Aktarımı (Maskeleme/Şifreleme YOK)
        for doc in mongo_cursor:
            raw_record = (
                str(doc.get('_id', '')),
                str(doc.get('musteri_id', '')),
                doc.get('ad_soyad', ''),
                doc.get('tckn', ''),
                doc.get('email', ''),
                doc.get('telefon', ''),
                doc.get('kredi_karti', ''),
                doc.get('bakiye', 0.0),
                doc.get('kayit_tarihi', None)
            )
            batch_data.append(raw_record)

            if len(batch_data) >= batch_size:
                insert_query = """
                    INSERT INTO masked_50m.unmasked_50m_customers 
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
                INSERT INTO masked_50m.unmasked_50m_customers 
                (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                VALUES %s
            """
            execute_values(pg_cursor, insert_query, batch_data)
            pg_conn.commit()
            total_inserted += len(batch_data)

        pg_cursor.close()
        pg_conn.close()
        
        # 5. Performans Raporu Oluşturma
        end_time = time.time()
        end_mem = process.memory_info().rss / (1024 * 1024)
        
        duration = end_time - start_time
        throughput = total_inserted / duration if duration > 0 else 0
        mem_diff = end_mem - start_mem

        logging.info("==================================================")
        logging.info("📊 TEZ ANALİZİ: BASELINE 50M (SIFIR GÜVENLİK MALİYETİ)")
        logging.info("==================================================")
        logging.info(f"Hedef Tablo:     unmasked_50m_customers")
        logging.info(f"Aktarılan Satır: {total_inserted}")
        logging.info(f"Geçen Süre:      {duration:.2f} saniye")
        logging.info(f"Hız (Throughput): {throughput:.2f} satır/sn")
        logging.info(f"RAM Kullanımı:   {mem_diff:.2f} MB")
        logging.info("==================================================")

    except Exception as e:
        logging.error(f"Baseline 50M aktarımı başarısız: {str(e)}")
        raise

# DAG Ayarları
default_args = {
    'owner': 'salih',
    'start_date': datetime(2026, 4, 25),
}

with DAG(
    dag_id='mongo_to_postgres_baseline_unmasked_50M',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tez', 'unmasked', 'baseline', '50M']
) as dag:

    transfer_task = PythonOperator(
        task_id='run_baseline_metrics',
        python_callable=transfer_baseline_unmasked
    )