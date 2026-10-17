# Wheel Airflow lokal

Folder ini menyimpan wheel Airflow 3.1.7 dan seluruh dependensi dari `requirements.txt` untuk **CPython 3.12, Linux x86_64**. Paket lain yang hanya tersedia di base image tidak seluruhnya diarsipkan.

Unduh atau perbarui wheel (memerlukan internet dan Docker):

```bash
bash scripts/download-pkgs.sh
```

Bangun image dan jalankan Airflow:

```bash
docker compose build airflow
docker compose up -d --no-deps airflow
```

Dockerfile memasang paket dari wheel lokal menggunakan `--no-index`. Build masih membutuhkan base image `apache/airflow:3.1.7-python3.12` di Docker lokal; bila belum tersedia, Docker mengunduhnya. Instalasi pip saat build tidak mengakses PyPI.

- `constraints-3.12.txt`: constraints resmi Airflow 3.1.7.
- `requirements.lock.txt`: versi paket yang ada di wheel lokal.
- `installed-airflow.txt`: inventaris seluruh paket pada container setelah build, termasuk paket bawaan base image.
- `SHA256SUMS`: checksum wheel yang diunduh pada sesi ini.
- `*.whl`: paket beserta dependensinya, termasuk Polars, openpyxl, dan xlsxwriter.

Jika `requirements.txt`, versi Python, arsitektur, atau versi Airflow berubah, siapkan ulang wheel yang sesuai. Untuk arsip baru yang bersih, pindahkan folder wheel lama terlebih dahulu agar tidak mencampur beberapa versi.
