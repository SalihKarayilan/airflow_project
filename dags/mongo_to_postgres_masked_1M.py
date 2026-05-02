import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# Maskeleme Fonksiyonları
def mask_email(email):
    if not email or "@" not in email:
        return email
    parts = email.split("@")
    name = parts[0]
    domain = parts[1]
    masked_name = name[0] + "***" + name[-1] if len(name) > 2 else "***"
    return f"{masked_name}@{domain}"

def mask_phone(phone):
    if not phone:
        return phone
    # Son 4 haneyi göster, gerisini yıldızla
    return "***-***-" + phone[-4:]

def mask_name(name):
    if not name:
        return name
    parts = name.split()
    masked_parts = [p[0] + "***" for p in parts]
    return " ".join(masked_parts)

def mask_tckn(tckn):
    if not tckn or len(tckn) != 11:
        return tckn
    # TCKN'nin ilk 2 ve son 2 hanesini göster, ortayı gizle
    return tckn[:2] + "*******" + tckn[-2:]

def mask_cc(cc):
    if not cc:
        return cc
    cc_str = str(cc)
    if len(cc_str) <= 4:
        return cc_str
    # Sadece son 4 haneyi bırak, geri kalan tüm haneleri yıldıza çevir
    return "*" * (len(cc_str) - 4) + cc_str[-4:]


def transfer_and_mask_data():
    batch_size = 10000  # Bellek şişmesin diye 10 binlik paketler
    
    logging.info("Veritabanı bağlantıları başlatılıyor...")
    
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    mongo_collection = mongo_client["tez_source_db"]["customers_raw"]
    logging.info("✅ MongoDB'ye başarıyla bağlandı!")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_company_db')
    pg_conn = postgres_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    logging.info("✅ PostgreSQL'e başarıyla bağlandı!")

    # Postgres Tablosunu Temizle (TRUNCATE)
    logging.info("Postgres tablosu temizleniyor (TRUNCATE)...")
    pg_cursor.execute("TRUNCATE TABLE masked_1m.masked_1m_customers;")
    pg_conn.commit()
    logging.info("Tablo başarıyla temizlendi. Veri aktarımı başlıyor.")

    # Mongo'dan Veri Çekme
    mongo_cursor = mongo_collection.find()
    
    batch_data = []
    total_inserted = 0

    # Verileri Döngüye Sok ve Maskele
    for doc in mongo_cursor:
        
        # Yeni tablo kolonlarına birebir uyan yapı
        masked_record = (
            str(doc.get('_id', '')),
            doc.get('musteri_id', ''),
            mask_name(doc.get('ad_soyad', '')),
            mask_tckn(doc.get('tckn', '')),
            mask_email(doc.get('email', '')),
            mask_phone(doc.get('telefon', '')),
            mask_cc(doc.get('kredi_karti', '')),
            doc.get('bakiye', 0.0),
            doc.get('kayit_tarihi', None)
        )
        batch_data.append(masked_record)

        # Paket (Batch) dolduğunda Postgres'e yaz
        if len(batch_data) >= batch_size:
            insert_query = """
                INSERT INTO masked_1m.masked_1m_customers 
                (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
                VALUES %s
            """
            execute_values(pg_cursor, insert_query, batch_data)
            pg_conn.commit()
            
            total_inserted += len(batch_data)
            logging.info(f"{total_inserted} satır maskelenip Postgres'e yazıldı.")
            batch_data.clear()

    # Kalan son verileri yaz
    if batch_data:
        insert_query = """
            INSERT INTO masked_1m.masked_1m_customers 
            (mongo_id, musteri_id, ad_soyad, tckn, email, telefon, kredi_karti, bakiye, kayit_tarihi)
            VALUES %s
        """
        execute_values(pg_cursor, insert_query, batch_data)
        pg_conn.commit()
        total_inserted += len(batch_data)
        logging.info(f"Kalan son batch yazıldı. Toplam aktarılan: {total_inserted}")

    pg_cursor.close()
    pg_conn.close()
    logging.info("🎉 Aktarım ve maskeleme işlemi başarıyla tamamlandı!")

# Airflow DAG Tanımlaması
default_args = {
    'owner': 'salih',
    'start_date': datetime(2026, 4, 25),
    'retries': 1,
}

with DAG(
    dag_id='mongo_to_postgres_masked_1M',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tez', 'masking', 'etl']
) as dag:

    mask_and_load_task = PythonOperator(
        task_id='mask_and_load_to_postgres',
        python_callable=transfer_and_mask_data
    