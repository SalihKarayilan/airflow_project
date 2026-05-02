Tezin Çerçevesi;

-Ana Odak: KVKK/GDPR Uyumlu Dinamik Veri Maskeleme Boru Hatlarında Şifreleme Maliyeti ve Performans Optimizasyonu.

-Sektörel Hedef: Üretim veritabanlarındaki verilerin analitik ekiplere aktarılırken anlık (on-the-fly) maskelenmesinin getirdiği operasyonel yükü ölçmek.

-Mimari Akış: Kaynak Veritabanı (MongoDB) → Orkestrasyon ve ETL (Apache Airflow DAG) → Anlık Maskeleme İşlemi (Python: Cryptography/Faker ile AES-256 veya FPE) → Hedef Veri Ambarı (PostgreSQL).
    -Test Edilecek Metrikler:
    -Aktarım hızı (Throughput)
    -Sistem kaynak tüketimi (CPU/RAM)
    -Gecikme süreleri (Latency)

-Stres Testi Ölçekleri: 1 Milyon, 10 Milyon ve 50 Milyon satırlık veri setleri.

-Nihai Araştırma Sorusu: Güvenlik katmanının veri mühendisliği operasyonlarına istatistiksel maliyeti nedir?
