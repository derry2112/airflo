Username: admin
Password: U5gxR5BQYXYEaAc5
# Airflow Project (tanpa Docker)

Starter project Apache Airflow 3 untuk development lokal, berjalan langsung di
Python virtual environment dengan database SQLite.

## Prasyarat

- Linux atau macOS
- Python 3.10–3.13 (disarankan Python 3.12)
- `pip` dan modul `venv`

Python 3.14 belum didukung oleh Airflow 3.1.7.

## Menjalankan proyek

```bash
cd airflow-project
./scripts/setup.sh
./scripts/start.sh
```

Buka <http://localhost:8080>. Perintah `airflow standalone` menampilkan username
dan password admin di terminal. Kredensial juga disimpan Airflow dalam file
`simple_auth_manager_passwords.json.generated` di folder proyek.

`start.sh` berjalan di foreground. Hentikan dengan `Ctrl+C`.

Jika beberapa versi Python terpasang, Anda dapat memilihnya secara eksplisit:

```bash
PYTHON_BIN=python3.12 ./scripts/setup.sh
```

## DAG contoh

`dags/example_etl.py` menjalankan alur berikut:

1. `extract` membuat data penjualan contoh.
2. `transform` menghitung total per produk.
3. `load` menulis hasil ke `data/sales_summary.csv`.

DAG dibuat dalam kondisi pause. Aktifkan `example_sales_etl` dari UI, atau uji
langsung dengan:

```bash
AIRFLOW_HOME="$PWD" .venv/bin/airflow dags test example_sales_etl
```

## Perintah berguna

```bash
./scripts/setup.sh
./scripts/start.sh

# Perintah CLI tambahan
AIRFLOW_HOME="$PWD" .venv/bin/airflow dags list
AIRFLOW_HOME="$PWD" .venv/bin/airflow db migrate
```

`Makefile` tersedia sebagai shortcut opsional jika GNU Make terpasang.

Untuk menambah dependency Python, isi `requirements.txt`, lalu jalankan kembali
`make setup`. Untuk deployment production, gunakan database dan executor yang
sesuai; SQLite dan `standalone` hanya ditujukan untuk development.

Terminal ID sementara menggunakan sourceregnum; foto spek tidak menunjukkan sumber terminal dengan jelas.
Beberapa transactiontype berbentuk teks sehingga mapping processing code menggunakan asumsi.
Nilai authorization code kosong masih fallback ke rc; perlu dipastikan apakah memang itu aturan bisnisnya.
Balance Inquiry sementara dipetakan sebagai Purchase karena tidak ada mapping khusus pada foto.