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


> Konfigurasi ini ditujukan untuk development/learning, bukan production.

Kamu bisa jawab seperti ini:
Posting file sudah disesuaikan dengan fixed-length specification HR, HS, DT, OA, TS, dan TR. Transaksi dikelompokkan berdasarkan kombinasi MID, MPAN, dan Terminal ID. Jika kombinasi tersebut sama, transaksi digabung dalam satu batch dengan satu HS, beberapa pasangan DT–OA, lalu satu TS summary. Jika salah satunya berbeda, sistem membuat grup HS–TS baru, tetapi masih dalam satu file POSTFLIN. HR hanya satu di awal dan TR hanya satu di akhir.

Kalau ditanya detail validasinya:
- Panjang record: HR 47, HS 89, DT 147, OA 184, TS 133, TR 74.
- Sequence record berurutan dalam satu file.
- Voucher DT dan OA selalu sama.
- Transaction sequence di-reset pada setiap batch.
- TS menghitung jumlah dan total amount per grup.
- TR menghitung jumlah dan total debit/credit seluruh file.
- Satu source file menghasilkan satu POSTFLIN.
- Kode lama tidak dihapus; tetap disimpan sebagai komentar.
Tambahkan catatan ini agar transparan:
Saat ini tabel MER_ACCEPTOR_POINT hanya menyediakan AP_ON_ID_10 dan MER_ACCEPTOR_POINT_ID. Karena belum ada kolom merchant number terpisah, Merchant Number dan Outlet Number sementara menggunakan MID yang sama. Mapping dapat dipisahkan setelah master merchant atau kolom mapping resminya tersedia.