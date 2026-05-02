import time
import uuid
import random
import logging
from datetime import datetime
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

default_args = {
    'owner': 'salih',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 0,
}

def seed_mongo_data(**kwargs):
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    
    total_records = conf.get('total_records', 50000000)
    batch_size = conf.get('batch_size', 10000)
    
    logging.info(f"--- {total_records} Satırlık Ham Veri Üretimi Başlıyor ---")
    start_time = time.time()

    try:
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        logging.info("MongoDB Atlas bağlantısı başarılı!")
    except Exception as e:
        logging.error(f"Bağlantı Hatası: {e}")
        raise e

    db = client["tez_source_db"]
    collection = db["customers_50M_raw"]

    fake = Faker('tr_TR')

    logging.info("Eski veriler temizleniyor...")
    collection.delete_many({})
    
    for i in range(0, total_records, batch_size):
        batch = []
        for _ in range(batch_size):
            doc = {
                "musteri_id": str(uuid.uuid4()),
                "ad_soyad": fake.name(),
                "tckn": fake.ssn(),
                "email": fake.email(),
                "telefon": fake.phone_number(),
                "kredi_karti": fake.credit_card_number(),
                "bakiye": round(random.uniform(100.0, 500000.0), 2),
                "kayit_tarihi": fake.date_time_between(start_date='-5y', end_date='now').isoformat()
            }
            batch.append(doc)
        
        collection.insert_many(batch)
        
        if (i + batch_size) % 100000 == 0:
            elapsed = time.time() - start_time
            logging.info(f"{i + batch_size} kayıt Atlas'a yazıldı. (Geçen süre: {elapsed:.2f} sn)")

    total_time = time.time() - start_time
    logging.info(f">>> İŞLEM TAMAMLANDI! Toplam {total_records} satır veri {total_time:.2f} saniyede aktarıldı. <<<")

with DAG(
    'seed_50M_mongodb_raw_data',
    default_args=default_args,
    description='Tez için MongoDB Atlas ortamına 1M ham veri basar',
    schedule=None,
    catchup=False,
    tags=['tez', 'veri_uretimi', 'mongodb'],
) as dag:

    generate_data_task = PythonOperator(
        task_id='generate_and_insert_to_mongo',
        python_callable=seed_mongo_data,
    )