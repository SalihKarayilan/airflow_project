# Apache Airflow Destekli ETL Süreçlerinde Dinamik Veri Maskeleme ve Pseudonymisation Uygulamaları

Bu depo, büyük veri mimarilerinde KVKK ve GDPR uyumluluğunu sağlamak amacıyla tasarlanmış, **Apache Airflow** orkestrasyonunda çalışan uçtan uca güvenli bir ETL (Extract-Transform-Load) boru hattı projesini içermektedir. 

Çalışma, Hacettepe Üniversitesi Bilişim Enstitüsü, Veri ve Bilgi Mühendisliği Tezsiz Yüksek Lisans Programı kapsamında Dönem Projesi olarak geliştirilmiştir.

## 🚀 Proje Özeti
Modern veri ambarı sistemlerinde hassas kişisel verilerin (PII) transferi esnasında güvenliğini sağlamak için iki temel strateji uygulanmış ve performans maliyetleri analiz edilmiştir:
1. **Dinamik Veri Maskeleme (DDM):** Rol tabanlı (admin, analyst vb.) kurallarla bellek üzerinde (in-memory) anlık veri gizleme.
2. **Kriptografik Takma Adlandırma (Pseudonymisation):** Airflow Variables entegrasyonu ile merkezi anahtar yönetimi kullanan AES-128 tabanlı Fernet şifreleme mimarisi.

Sistem; 1 Milyon, 10 Milyon ve 50 Milyon satırlık devasa veri kümeleri (MongoDB'den PostgreSQL'e) üzerinde test edilmiş ve algoritmaların işlemci (vCPU), RAM ve aktarım hızı (throughput) üzerindeki etkileri deneysel olarak ölçülmüştür.

## 🏗️ Mimari ve Teknolojiler

* **Orkestrasyon:** Apache Airflow 2.10
* **Kaynak Veritabanı:** MongoDB (Distribütif NoSQL Küme Simülasyonu)
* **Hedef Veri Ambarı:** PostgreSQL 13 (İlişkisel DWH)
* **Kriptografi:** Python `cryptography` (Fernet)
* **Altyapı:** Docker & Docker Compose (İzole Ağ Mimarisi)
* **Veri Üretimi:** Python `Faker` kütüphanesi

## ⚙️ Kurulum ve Çalıştırma

Projeyi yerel ortamınızda çalıştırmak için aşağıdaki adımları izleyin:

### 1. Sistem Gereksinimleri ve Kaynak Optimizasyonu
Özellikle 50 Milyonluk veri setinde OOM (Out-of-Memory) hatalarını önlemek için WSL2 (Windows Subsystem for Linux) kaynaklarını sabitlemeniz önerilir. Kullanıcı dizininizde (`~/.wslconfig`) aşağıdaki yapılandırmayı oluşturun:
```ini
[wsl2]
memory=10GB
processors=4
swap=2GB