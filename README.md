# Airflow QR Recon — panduan lingkungan lokal

Panduan ini mengikuti `compose.yaml` utama: Airflow **3.1.7 / Python 3.12**,
PostgreSQL, Oracle, FTP, dan SFTP lokal. Adapter kompatibilitas sudah digabung
ke Compose utama; tidak perlu `compose.compat.yaml`.
Server SSH PWC dummy menggunakan Compose terpisah di `scripts/pwc-ssh-demo/`.

## Versi runtime

Runtime container diverifikasi pada 6 Oktober 2026:

| Komponen | Versi |
|---|---|
| Apache Airflow | **3.1.7** |
| Python | **3.12.12** |
| Base image Docker | `apache/airflow:3.1.7-python3.12` |

Untuk mengecek versi runtime yang sedang berjalan:

```bash
docker compose exec -T airflow airflow version
docker compose exec -T airflow python --version
```

## Upgrade dari Airflow 2

Referensi: [panduan upgrade resmi Airflow 3.1.7](https://airflow.apache.org/docs/apache-airflow/3.1.7/installation/upgrading_to_airflow3.html).

Image lokal dibangun dari `apache/airflow:3.1.7-python3.12`; seluruh provider
mengikuti constraints resmi versi yang sama. Volume `airflow2-data` tetap digunakan
agar history dan Connections tersedia setelah migrasi. Volume `airflow-data` lama
juga tetap disimpan.

Sebelum upgrade environment yang sudah berisi data, hentikan Airflow dan backup:

```bash
bash scripts/init-env.sh
docker compose stop airflow
mkdir -p backups
docker run --rm --user 0 --entrypoint tar -v airflow-project_airflow2-data:/source:ro -v "$PWD/backups:/backup" apache/airflow:2.10.5-python3.12 -czf /backup/airflow2-before-upgrade.tgz -C /source .
docker compose build airflow
docker compose run --rm --no-deps airflow airflow config update --fix
docker compose run --rm --no-deps airflow airflow db migrate
docker compose up -d
```

Nama volume mengikuti nama project Compose; sesuaikan bila project name berbeda.
Airflow 3 menggunakan Simple Auth Manager untuk standalone. Password admin berada
di `simple_auth_manager_passwords.json.generated`; password UI Airflow 2 tidak berlaku.
JWT secret acak dibuat oleh `scripts/init-env.sh` di `.env` (diabaikan Git).
DAG memakai `logical_date` (fallback `dag_run.run_after` untuk trigger manual),
operator dari standard provider, dan Task SDK.
SLA lama dihapus karena tidak didukung Airflow 3; `execution_timeout` tetap aktif.

## 1. Prasyarat dan startup

Siapkan Docker Engine yang berjalan, Docker Compose, client SSH, serta port host
8080, 5433, 1521, dan 2222 yang tersedia. Build awal memerlukan internet.
Jalankan seluruh perintah berikut dari root repository:

```bash
cd /home/enigma_camp/myproject/airflow-project
bash scripts/init-env.sh
mkdir -p data/pwrcard/home/usr/data data/ACQ_MTI/ClearingRintisQRIS
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail 50 airflow
```

Tunggu startup selesai; provider sudah dipasang saat build image. Periksa kesehatan Airflow:

```bash
docker compose exec -T airflow curl -fsS http://localhost:8080/api/v2/monitor/health
```

Buka [Airflow](http://localhost:8080). Akun standalone biasanya `admin`;
lihat kredensial yang dibuat pada log startup atau file lokal container:

```bash
docker compose exec -T airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Gunakan kredensial dari environment yang sedang berjalan, bukan catatan lama.

## 2. Service dan lokasi data

| Service | Container | Akses dari host | Akses antar-container |
|---|---|---|---|
| Airflow | airflow-web | localhost:8080 | airflow:8080 |
| PostgreSQL | airflow-postgres | localhost:5433 | postgres:5432 |
| Oracle | airflow-oracle | localhost:1521 | oracle:1521 |
| FTP | airflow-ftp | Tidak dipublikasikan | ftp:21 |
| SFTP PWC | airflow-sftp-pwc | Tidak dipublikasikan | sftp-pwc:22 |
| SSH dummy (Compose terpisah) | pwc-ssh-demo-pwc-ssh-demo-1 | localhost:2222 | Port SSH internal 22 |

| Data | Lokasi lokal / penyimpanan |
|---|---|
| Source QR Recon | `data/ACQ_MTI/ClearingRintisQRIS/` |
| File download dan hasil mapping | `data/artifacts/data_payment/acquiring/` |
| File yang dikirim ke PWC lokal | `data/pwrcard/home/usr/data/` |
| Tujuan FTP MTI | `data/ftp-destination/sftpdata/` |
| Metadata dan output internal Airflow | Volume `airflow2-data` |
| Database PostgreSQL | Volume `postgres-data` |
| Database Oracle | Volume `oracle-data` |
| Response SSH dummy | Volume `responses` pada project SSH demo |

SFTP memakai chroot: folder host `data/pwrcard` dipasang di
`/home/airflow/pwrcard`, tetapi client melihat `/pwrcard/home/usr/data/`.
SSH dummy memasang input yang sama secara read-only pada `/pwrcard/home/usr/data/`.

## 3. Database dan data dummy

Pada volume PostgreSQL baru, file dalam `postgres/init/` dijalankan otomatis
berdasarkan urutan nama. Pada volume yang sudah ada, script init tidak otomatis
berjalan ulang. Terapkan perubahan yang diperlukan secara manual:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d db_rekon_uat < postgres/init/03_ensure_way4_query_columns.sql
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d db_rekon_uat < postgres/init/04_seed_way4_qr_test.sql
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d postgres < postgres/init/05_qris_enrichment_local.sql
```

| Script | Fungsi |
|---|---|
| `01_on_doc_transaction.sql` | Membuat schema dan tabel Way4 |
| `02_seed_on_doc_transaction.sql` | Seed contoh awal; pembaruan tanggal untuk data seed |
| `03_ensure_way4_query_columns.sql` | Menambahkan kolom query yang belum tersedia |
| `04_seed_way4_qr_test.sql` | Seed 12 baris untuk hari ini dan kemarin, Asia/Jakarta |
| `05_qris_enrichment_local.sql` | Membuat DB `db_acq_psql`, tabel `merchant_tm` dan `qris_transaction_log_tt` |
| `06_seed_onus_local.sql` | Fixture ONUS dan enrichment untuk hari ini, kemarin, serta 19 Oktober 2026 |

Seed 04 mencakup CH Payment/Credit RC 0 serta RC 68/82 dengan Retail pendukung:
query menghasilkan 4 transaksi per tanggal. Script 05 menyediakan RRN historis **11–12 September 2026** serta enrichment
untuk hari ini dan kemarin (Asia/Jakarta), sesuai seed Way4 04. Jalankan ulang
script 04 dan 05 saat menguji pada hari lain.

Oracle menginisialisasi `oracle/init/01_powercard.sql` pada database baru.
Lookup merchant Rintis membutuhkan MPAN yang cocok dengan `MER_ACCEPTOR_POINT`;
seed saat ini hanya menyediakan satu pasangan merchant contoh.

Untuk konfigurasi `dag_postingfile` yang memakai `source_connection_type = SFTP`,
input harus tersedia di `data/pwrcard/home/usr/data/ACQ/MTI/ClearingRintisQRIS/`.
File di `data/ACQ_MTI/ClearingRintisQRIS/` hanya tersedia lewat FTP dan tidak otomatis
terbaca oleh koneksi SFTP. Nama file harus cocok dengan mask tanggal di DAG.
Status run sukses dengan daftar `Unprocessed files: []` berarti belum ada input yang diproses.
User SFTP lokal memiliki UID 1000; direktori tujuan MTI juga harus dapat ditulis oleh user tersebut.

### Data ONUS lokal dan query

Jalur ONUS menggunakan transaksi Way4 pada `sw_replicate.on_doc_transaction`.
Fixture tambahan tersedia di [`06_seed_onus_local.sql`](postgres/init/06_seed_onus_local.sql),
termasuk enrichment pada `db_acq_psql`. Jalankan setelah script 03 dan 05:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d db_rekon_uat < postgres/init/06_seed_onus_local.sql
```

Script dapat dijalankan ulang tanpa menggandakan fixture. Tanggal fixture adalah
hari ini, kemarin (Asia/Jakarta), dan 19 Oktober 2026 agar cocok dengan contoh QR Recon.
Per tanggal tersedia enam baris sumber: empat transaksi ONUS dan dua Retail pendukung.
MPAN enrichment memakai merchant PWC lokal `9360002801111177646`; MID `111117764535`.

| Jenis | RC | Nominal | Hasil query |
|---|---|---:|---|
| CH Payment | 0 | 10.000 | Debit |
| Credit | 0 | 20.000 | Kredit |
| CH Payment | 68 | 30.000 | Debit, didukung Retail RC 0/reversal 0 dengan RRN sama |
| Credit | 82 | 40.000 | Kredit, didukung Retail RC 0/reversal 0 dengan RRN sama |

Query yang digunakan disimpan di [`onus_way4.sql`](references/sql/onus_way4.sql).
Kolom dan filter disalin dari `_get_way4_data_new` pada `dag_postingfile.py`;
placeholder hanya diubah ke format `psql` agar dapat dijalankan langsung:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d db_rekon_uat -v period=20261019 -v pan_prefix='936000281%' < references/sql/onus_way4.sql
```

Query mengambil `connection_acq = 'T'`, issuer `ACQQR/a/O`, PAN berawalan
`936000281`, serta CH Payment/Credit dengan RC 0 atau RC 68/82 yang memiliki
Retail sukses. Query juga mengambil fixture lain yang memenuhi filter tersebut;
fixture ONUS tambahan memiliki RRN tanggal + suffix `910001`, `910002`, `910003`, `910005`.
DAG saat ini mengisi parameter tanggal query dengan **kemarin menurut waktu proses**,
meskipun tanggal file/logical date berbeda. Penambahan fixture ini tidak mengubah aturan itu.

Contoh POSTFLIN khusus ONUS dibuat di `data/pwrcard/home/usr/data/onus/` dengan
nama `POSTFLIN_ONUS_SAMPLE_20261019.txt`. File contoh berisi
empat transaksi (dua debit dan dua kredit), menggunakan generator dan lookup merchant
DAG yang sama. Ini artefak uji lokal; alur DAG utama tetap menggabungkan Way4 dan Rintis
pada POSTFLIN PWC sesuai implementasi existing.

## 4. Airflow Connections

Buka **Admin → Connections**. Connection dari environment Compose bisa tidak
muncul sebagai baris pada UI, tetapi tetap dapat digunakan oleh hook.

| Conn ID | Tipe | Host / port | Database / catatan |
|---|---|---|---|
| `db_rekon_uat` | Postgres | postgres:5432 | db_rekon_uat; tersedia melalui Compose |
| `db_acq_psql` | Postgres | postgres:5432 | db_acq_psql; tambahkan jika belum ada |
| `FTP_EBSP` | FTP | ftp:21 | Source; tersedia melalui Compose |
| `FTP_MII` | FTP | ftp:21 | Alias lama di Compose |
| `FTP_MTI` | FTP | ftp:21 | ID yang dipakai workflow; tambahkan jika belum ada |
| `SFTP_PWC_POST` | SFTP | sftp-pwc:22 | Tersedia melalui Compose |
| `pwcDB` | Oracle | oracle:1521 | Login POWERCARD; service FREEPDB1 |

Untuk database/FTP lokal, username dan password demo adalah `airflow`.
Tambahkan koneksi berikut **hanya jika ID belum ada**:

```bash
docker compose exec -T airflow airflow connections add db_acq_psql --conn-type postgres --conn-host postgres --conn-schema db_acq_psql --conn-login airflow --conn-password airflow --conn-port 5432
docker compose exec -T airflow airflow connections add FTP_MTI --conn-type ftp --conn-host ftp --conn-login airflow --conn-password airflow --conn-port 21
docker compose exec -T airflow airflow connections add pwcDB --conn-type oracle --conn-host oracle --conn-login POWERCARD --conn-password powercard --conn-port 1521 --conn-extra '{"service_name":"FREEPDB1"}'
```

Koneksi yang dibuat melalui UI/CLI disimpan di metadata Airflow. Jika sudah ada,
periksa/edit melalui UI, jangan menambah ulang dengan ID yang sama.
Kredensial di atas hanya untuk service demo lokal.

## 5. Adapter lokal tanpa perubahan kode existing

`airflow_runtime_compat/airflow_local_settings.py` dimuat melalui `PYTHONPATH`
di Compose. Adapter menerima constructor lama `DBConnection(**kwargs_db_source)`,
menyediakan `execute_fetch_many()`, dan menerjemahkan `:rrns` menjadi `%(rrns)s`.
Adapter mengikuti konfigurasi `type=postgres` dan Connection ID `db_acq_psql`.
Ia tidak menghubungkan PostgreSQL ke MsSqlHook dan tidak membutuhkan `mssql_qris`.

Adapter membungkus callable task pada DAG QR Recon di proses lokal. Adapter
database tetap dipakai bersama API DAG Airflow 3. Kebijakan runtime lama tetap dipakai:
**task `get_way4_data` terpisah masih berupa placeholder yang mengembalikan `[]`.**
Pemanggilan `get_way4_data_new()` dari proses split adalah jalur berbeda dan tetap
berjalan. Status DAG sukses tidak membuktikan seluruh jalur produksi sudah diuji.

Periksa runtime yang aktif:

```bash
docker compose exec -T airflow python -c 'import airflow_local_settings; print(airflow_local_settings.__file__)'
```

Hasil yang diharapkan: `/opt/airflow/runtime-compat/airflow_local_settings.py`.

## 6. Menjalankan pengujian

Mulai dari DAG **`local_way4_mapping_check`** di UI. DAG ini tidak terjadwal;
klik Trigger DAG untuk menguji query, enrichment dummy, mapping, dan file output.
Tidak ada upload atau posting. Task `check_mapping` mengembalikan lokasi output,
jumlah transaksi, ukuran, dan SHA-256 melalui log/XCom. Output berada dalam
container di `/opt/airflow/way4-local-*/POSTFLIN_LOCAL_DUMMY.txt`.
DAG uji ini memakai enrichment sintetis CTE sendiri, sehingga suksesnya tidak
membuktikan koneksi enrichment pada DAG utama sudah tersedia.

Untuk menguji alur file utama:

1. Siapkan file QR Recon valid di folder source lokal; tanggal nama file harus
   sesuai tanggal yang dipakai workflow (`fetch_date=0` saat ini).
2. Pastikan semua Connection di atas mengarah ke service lokal dan lookup merchant tersedia.
3. Buka DAG `etl_OCBC_MTI_RINTIS_ProcessFileRintisQRRecon`, lalu Trigger DAG.
4. Periksa task yang gagal melalui **Logs**; lihat **Import Errors** jika DAG tidak muncul.
5. Periksa hasil POSTFLIN di `data/pwrcard/home/usr/data/`.

DAG utama melakukan upload ke FTP/SFTP yang dikonfigurasi. Task `check_chk` dapat
melewati proses jika marker `.chk` sudah ditemukan. Periksa marker dan tanggal
sebelum menganggap task yang skipped sebagai error. Script posting Bash/SSH belum
otomatis dirangkai sebagai task pada DAG utama.

## Setting global generator POSTFLIN

Edit setting di [`PostFile_QRRecon.py`](dags/data_payment/workflow/PostFile_QRRecon.py),
lalu trigger run baru. File hasil sebelumnya tidak berubah.

- `SPACE`: satu karakter padding untuk field teks dan field kosong (default spasi).
- `MERCHANT_OUTLET_SPACES = 1`: tambahan satu spasi antara merchant number dan
  outlet number pada HS/TS. Panjang HS menjadi 90 dan TS 134; nilai 0 mengembalikan
  layout sebelumnya (89/133).
- `POSTING_FIELDS`: nilai tetap per record HR/HS/DT/OA/TR; `""` menghasilkan field kosong.
- Contoh: `POSTING_FIELDS["DT"]["pos_entry_mode"] = "012"`,
  `POSTING_FIELDS["OA"]["dcc_indicator"] = "N"`.

Nilai otomatis di-padding kanan hingga panjang field yang ditetapkan generator.
Nilai terlalu panjang, non-ASCII, atau mengandung newline akan ditolak. Panjang
record divalidasi sesuai jumlah spasi tambahan; zero-padding angka tetap sama. Perubahan nilai
harus sesuai spesifikasi penerima; `SPACE = " "` menghasilkan padding spasi.
Tanggal, nominal, merchant, dan processing code tetap diambil dari mapping transaksi.

## 7. Menjalankan Bash lokal

Ganti nama input berikut dengan file yang benar-benar tersedia:

```bash
bash scripts/run_pwc_posting.sh POSTFLIN_20260912014606.txt RESPONSE_preview.txt
```

Default hanya preview. Untuk menjalankan script PWC asli yang tersedia **di lokal**:

```bash
PWC_SCRIPT=/path/absolut/PWC.sh bash scripts/run_pwc_posting.sh --execute POSTFLIN_20260912014606.txt RESPONSE_result.txt
```

`PWC_DATA_DIR` dapat mengganti direktori input. Nama response harus berbeda dari
input dan belum ada. Runner memakai tanggal hari ini Asia/Jakarta (`DDMMYYYY`):

```text
bash <PWC_SCRIPT> load_posting_new_file <DDMMYYYY> <FILENAME> DE-FILE V002 N <RESPONSE_FILENAME> NULL NULL NULL NULL
```

Path/script dan kontrak argumen server asli tetap perlu diverifikasi sebelum
posting nyata. Runner tidak mencegah posting ulang input dengan nama response baru.

## 8. Membuat dan menguji server SSH PWC dummy

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml up -d --build
docker compose -f scripts/pwc-ssh-demo/compose.yaml ps
```

Host `127.0.0.1`, port `2222`, username `pwcdemo`, password `demo-local-only`.
Nama otomatis `pwc-ssh-demo-pwc-ssh-demo-1` terdiri dari project, service, dan
nomor instance. Tidak ada `container_name` eksplisit pada konfigurasi saat ini.

Bandingkan fingerprint saat pertama kali login dengan public key container:

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml exec pwc-ssh-demo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
bash scripts/run_pwc_ssh_demo.sh POSTFLIN_20260912014606.txt RESPONSE_demo.txt
ssh -p 2222 pwcdemo@127.0.0.1 'cat /pwrcard/home/usr/responses/RESPONSE_demo.txt'
```

Setting SSH ada di bagian atas `scripts/run_pwc_ssh_demo.sh`.
`REMOTE_SCRIPT=/opt/pwc/PWC.sh` menunjuk script dummy di container, disalin dari
`scripts/pwc-ssh-demo/PWC.sh` saat build. Respons bertanda `SIMULATION_ONLY` dan
`NO_TRANSACTIONS_POSTED`; gunakan nama response baru untuk percobaan berikutnya.

Panduan detail, shell container, serta opsi penamaan tetap tersedia di
[README SSH demo](scripts/pwc-ssh-demo/README.md).

## 9. Troubleshooting dan operasi harian

```bash
docker compose ps
docker compose logs --tail 100 airflow
docker compose exec -T airflow airflow dags list-import-errors
docker compose exec -T postgres psql -U airflow -d db_acq_psql -c 'SELECT count(*) FROM qris_transaction_log_tt;'
```

| Gejala | Pemeriksaan / tindakan lokal |
|---|---|
| `airflow.sdk` tidak ditemukan | Build ulang image Airflow 3: `docker compose up -d --build` |
| Provider MSSQL tidak ditemukan | Pastikan startup pip berhasil; dependency MSSQL sudah ada pada Compose utama |
| `unexpected keyword argument type` | Pastikan runtime-compat aktif dan jalankan ulang task melalui DAG QR Recon |
| `db_acq_psql isn't defined` | Tambahkan Connection sesuai bagian 4 |
| Tabel enrichment tidak ditemukan | Jalankan SQL 05 pada instance PostgreSQL lokal yang sama |
| `dict is not a sequence` | Cocokkan placeholder query dengan parameter; jalur Way4 baru memakai named placeholders |
| Mapping account type kosong | Periksa RRN Way4 memiliki enrichment yang sesuai, khususnya tanggal seed |
| Browser belum tersedia | Tunggu startup, periksa log serta health; container Up belum tentu webserver siap |
| Permission denied Docker | Pastikan user terminal mempunyai akses ke daemon Docker |
| SSH connection refused | Periksa service SSH demo berjalan dan port 2222 tersedia |

Menghentikan service tanpa menghapus volume database/response:

```bash
docker compose down
docker compose -f scripts/pwc-ssh-demo/compose.yaml down
```

Hindari `down -v` jika ingin mempertahankan data. Untuk menyalakan kembali, gunakan
perintah `up -d` pada masing-masing Compose. Jangan menjalankan dua environment
Airflow sekaligus pada port 8080.

## Arsip dokumentasi sebelumnya

Bagian di bawah dipertahankan sebagai referensi historis. Instruksi Airflow 3 tanpa
Docker dan tabel mapping lama **bukan acuan untuk runtime Docker saat ini**.
Fungsi mapping lama dan `map_way4_to_posting_records_new` memiliki aturan berbeda;
lihat source aktif dan spesifikasi bisnis sebelum memakai tabel lama.

<details>
<summary>Buka dokumentasi lama</summary>

Username: admin
Password: sDmt3qqUFFsn8xUz
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

<!--
Catatan lama sebelum dokumentasi dirapikan:
- Terminal ID sementara menggunakan sourceregnum; foto spek tidak menunjukkan sumber terminal dengan jelas.
- Beberapa transactiontype berbentuk teks sehingga mapping processing code menggunakan asumsi.
- Nilai authorization code kosong masih fallback ke rc; perlu dipastikan apakah memang itu aturan bisnisnya.
- Balance Inquiry sementara dipetakan sebagai Purchase karena tidak ada mapping khusus pada foto.
-->

## Perbedaan Mapping Rintis dan Way4

Rintis dan Way4 menggunakan sumber data serta aturan mapping yang berbeda. Keduanya
diubah menjadi format POSTFLIN, digabung dalam satu file, lalu dikirim ke PWC.

### Ringkasan Perbedaan

<!--
Diagram lama sebelum ringkasan diubah menjadi tabel:

Data Rintis ──→ mapping Rintis ──┐
                                 ├──→ POSTFLIN gabungan ──→ PWC
Data Way4   ──→ mapping Way4   ──┘

Hasil split Rintis ──→ file split + .chk ──→ MTI
-->

| Aspek | Rintis | Way4 |
|---|---|---|
| Sumber data | File `QR_RECON_*_ISS` dari FTP | PostgreSQL `sw_replicate.on_doc_transaction` |
| Cara membaca | Parsing record `RH`, `DH`, dan `RT` | Query berdasarkan `period` dan `connection_acq` |
| Model transaksi | `ReconRecordData` langsung dari file | Hasil query dipetakan menjadi `ReconRecordData` |
| Identifikasi merchant | Lookup `merchant_pan` ke PowerCARD | Sementara memakai 15 karakter pertama `pan` |
| Hasil pemrosesan | File hasil split Rintis dan blok POSTFLIN Rintis | Blok POSTFLIN Way4 |
| Blok record | `HS → DT → OA → TS` | `HS → DT → OA → TS` |
| Penggabungan | Digabung dengan blok Way4 | Digabung dengan blok Rintis |
| Tujuan utama | Hasil split dan `.chk` dikirim ke MTI | Tidak dikirim langsung ke MTI |
| Tujuan POSTFLIN | File combine dikirim ke PWC | File combine dikirim ke PWC |
| File `.chk` MTI | Dibuat khusus untuk hasil split Rintis | Tidak dibuat untuk Way4 atau hasil combine |

### Mapping Field POSTFLIN

| Field POSTFLIN | Rintis | Way4 |
|---|---|---|
| Merchant Number | Hasil lookup `merchant_pan` ke PowerCARD | 15 karakter pertama dari `pan` untuk sementara |
| Outlet Number | MID hasil lookup PowerCARD | `merchantid` |
| Terminal ID | `terminal_id` | `sourceregnum` |
| Batch Capture Date | Tanggal proses saat ini | `period` |
| Batch Date & Time | `transaction_date + transaction_time` | `settlementdate + periodtime` |
| Batch Currency | `transaction_amount_currency` | `settlementcurrency` |
| Processing Code | `processing_code` | Diturunkan dari `transactiontype` |
| Card Number | `customer_pan` | `pan` |
| Transaction Amount | `transaction_amount` | `reconamount` |
| Transaction Currency | `transaction_amount_currency` | `transactioncurrency` |
| Service/Convenience Fee | `convenience_fee` | `transactionchargeamount` |
| Authorization Code | `approval_code` | `auth_code`, fallback ke `rc` |
| Reversal Flag | Mapping transaksi Rintis | `F` jika `reversalseq > 0`, selain itu `N` |
| RRN | `retrieval_reference_number` | `rrn` |

### Mapping Processing Code Way4

Processing code terdiri dari prefix transaksi dua digit dan empat digit account
type. Implementasi sementara menggunakan account type `0000`.

| Kategori | Nilai sumber | Hasil sementara |
|---|---|---|
| Purchase | `Retail`, `Purchase`, `CHK_RCPT`, `CH Payment` | `000000` |
| Purchase dengan prefix sumber | `00`, `09`, `19`, `95`, `96` | `<prefix>0000` |
| Visa service fee | `12` | `120000` |
| Refund/Credit Voucher | `Refund`, `Credit Voucher`, `20`, `29` | `200000` atau `<prefix>0000` |
| Withdrawal | `Withdrawal`, `01` | `010000` |
| Cash | `Cash`, `11`, `17`, `28` | `110000` atau `<prefix>0000` |
| Deposit | `Deposit`, `21` | `210000` |
| Transfer | `Transfer`, `IB_TRF`, `40` | `400000` |
| Loan | `Loan`, `92`, `93` | `920000` atau `<prefix>0000` |
| Redemption | `Redemption`, `94` | `940000` |
| Convenience Fee | `Convenience Fee`, `97` | `970000` |
| Belum terdefinisi | `Balance Inquiry` atau nilai lain | Sementara `000000` |

### Struktur File Gabungan

Setiap sumber menghasilkan satu atau lebih batch `HS → DT → OA → TS`. File akhir
memiliki satu `HR` dan satu `TR`:

```text
HR
  HS → DT → OA → ... → TS  (Rintis)
  HS → DT → OA → ... → TS  (Way4)
TR
```

| Record | Field terakhir | Posisi | Panjang field | Total record |
|---|---|---:|---:|---:|
| HR | Tokenization Indicator | 47 | 1 | 47 |
| HS | Batch Type | 89 | 1 | 89 |
| DT | Single Message Indicator | 147 | 1 | 147 |
| OA | Reserved for Future Use/RRN | 173 | 12 | 184 |
| TS | Net Amount Credit | 116 | 18 | 133 |
| TR | Net Amount Debit | 57 | 18 | 74 |

### Nilai Tetap yang Digunakan

| Field | Nilai |
|---|---|
| POS Data | `100001154110` |
| POS Entry Mode | `012` |
| POS Condition Code | `00` |
| Batch Type | `P` |
| Currency Exponent | `1` |
| Single Message Indicator | `Y` |

### Tujuan Pengiriman

| Tujuan | File yang dikirim |
|---|---|
| MTI | File hasil split Rintis dan marker `.chk` Rintis |
| PWC | POSTFLIN hasil combine Rintis dan Way4 |

### Catatan Mapping Sementara

> Bagian berikut belum dianggap sebagai mapping bisnis final.

1. Merchant Number Way4 menggunakan 15 karakter pertama dari `pan`.
2. Terminal ID Way4 menggunakan `sourceregnum`.
3. Mapping `transactiontype` berbentuk teks masih perlu validasi bisnis.
4. Authorization code kosong menggunakan `rc` sebagai fallback.
5. `Balance Inquiry` sementara diperlakukan sebagai Purchase.

### Mapping Way4 yang Belum Jelas

| Field/Aturan | Implementasi Saat Ini | Hal yang Perlu Dikonfirmasi |
|---|---|---|
| Merchant Number | 15 karakter pertama dari `pan` | Apakah PAN memang boleh dipotong, memakai 15 digit terakhir, atau harus melalui lookup merchant PowerCARD? |
| Outlet Number | `merchantid` | Apakah `merchantid` selalu merupakan Merchant Acceptor Point Number pada HS/TS? |
| Terminal ID | `sourceregnum` | Foto spesifikasi belum menunjukkan sumber Terminal ID secara tegas. |
| Card Number | `pan` | Perlu dipastikan apakah `pan` berisi CPAN/card PAN dan boleh sekaligus menjadi sumber Merchant Number. |
| Batch Capture Date | `period` | Perlu dipastikan apakah menggunakan `period` atau business date lain. |
| Batch Date & Time | `settlementdate + periodtime` | `settlementdate` tidak memiliki komponen waktu; penggunaan `periodtime` masih perlu konfirmasi. |
| Processing Code | Prefix dua digit dari `transactiontype`, account type `0000` | Account type pada posisi 3–6 dan mapping nilai teks belum tersedia secara lengkap. |
| `Balance Inquiry` | Sementara dipetakan ke Purchase (`000000`) | Tidak ada kategori Balance Inquiry pada foto mapping processing code. |
| Transaction Amount | `reconamount` | Perlu dipastikan penggunaan `reconamount`, bukan `transactionamount` atau `settlementamount`, untuk seluruh jenis transaksi. |
| Transaction Currency | `transactioncurrency` | Perlu aturan ketika berbeda dengan `settlementcurrency`. |
| Service/Convenience Fee | `transactionchargeamount` | Perlu dipastikan currency fee dan perlakuan sign/decimal-nya. |
| Authorization Code | `auth_code`, fallback ke `rc` | `rc` adalah response code dan belum tentu valid sebagai authorization code 6 karakter. |
| Response Code | `rc`, dipenuhi hingga 2 karakter | Data contoh memiliki nilai satu karakter seperti `0`; format normalisasinya perlu dikonfirmasi. |
| Reversal Flag | `F` jika `reversalseq > 0`, selain itu `N` | Perlu aturan untuk membedakan full reversal (`F`) dan partial reversal (`P`). |
| Transaction Sign | Prefix `20`/`29` menjadi `D`, lainnya `C` | Perlu konfirmasi untuk seluruh kategori refund, reversal, cash, dan transfer. |
| Batch Number | Auto-generated dan saat ini dimulai dari `00000001` | Perlu dipastikan scope increment: per merchant, per file, per terminal, atau per hari. |

</details>

### Instalasi package dari wheel lokal

Wheel Airflow 3.1.7 dan dependensi proyek untuk Python 3.12/Linux x86_64 disimpan di `pkgs/`. Dockerfile menggunakan `pip install --no-index --find-links=/tmp/pkgs` agar instalasi package saat build memakai file lokal.

```bash
# Unduh wheel dan buat lock versi (memerlukan internet):
bash scripts/download-pkgs.sh
# Pasang wheel ke image, lalu terapkan image baru:
docker compose build airflow
docker compose up -d --no-deps airflow
```

Base image Airflow tetap harus tersedia di Docker. Rincian ada di [pkgs/README.md](pkgs/README.md).

### Memindahkan period ONUS untuk uji pada database existing

Gunakan [onus_update_period_existing.sql](references/sql/onus_update_period_existing.sql)
pada database Way4. Ganti `source_period` dan daftar RRN dengan transaksi yang akan diuji.
`target_period` default adalah kemarin di Asia/Jakarta; sesuaikan dengan tanggal yang
benar-benar dihitung proses Airflow existing (`datetime.today() - timedelta(days=1)`).
Script tidak bergantung pada marker dummy `add_data` dan tidak menambahkan data baru.

Script menampilkan data sebelum perubahan, melewati RRN/jenis transaksi yang sudah
ada pada tanggal tujuan, memperbarui `period` untuk transaksi terpilih beserta Retail
pendukungnya, lalu menampilkan transaksi yang lolos filter ONUS. Default akhir adalah
`ROLLBACK`; setelah preview sesuai, ubah baris terakhir menjadi `COMMIT` dan jalankan
ulang seluruh file untuk menyimpan perubahan. Simpan hasil preview sebagai catatan
`docid` dan tanggal asal. Contoh menjalankan preview di lingkungan lokal:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U airflow -d db_rekon_uat < references/sql/onus_update_period_existing.sql
```

Jalankan [onus_check_enrichment_existing.sql](references/sql/onus_check_enrichment_existing.sql)
secara terpisah pada database enrichment, dengan daftar RRN yang sama. Jalankan
[onus_check_merchant_existing.sql](references/sql/onus_check_merchant_existing.sql)
pada Oracle PWC, mengisi bind `:mpan` dari hasil enrichment. Tidak ada perubahan data
pada kedua query pengecekan tersebut. `MERCHANT_NUMBER` wajib terisi dan valid sesuai
validasi DAG. Update `period` tidak mengubah RRN sehingga enrichment memakai RRN existing.

Sesudah perubahan disimpan, pastikan file QR Recon berada di source FTP/SFTP yang
benar dan nama file sesuai mask tanggal run. ONUS saat ini diproses saat DAG menemukan
input QR Recon; update database saja tidak menghasilkan POSTFLIN ketika daftar input kosong.

### Hasil dag_postingfile melalui S3ArtefactHook

`process_file` mengunggah hasil split Rintis, hasil split PWC, dan POSTFLIN ke S3
menggunakan `S3ArtefactHook.put_file_to_s3`. Task `send_file` mengunduh objek S3
ke direktori sementara sebelum mengirimnya ke tujuan FTP/SFTP existing.
XCom menyertakan `mti_s3_url`, `pwc_s3_url`, dan `posting_s3_urls`.

- `result_local_path` dan `result_local_path_PWC` tetap menjadi staging untuk
  generator existing dan pemindaian nomor PTIN. File lokal tidak dihapus sesudah upload.
- `result_s3_app_rintis = data_payment/acquiring/result/rintis`.
- `result_s3_app_pwc = data_payment/acquiring/result/rintisPWC`.
- Pola key: `artefacts/<result_s3_app>/<YYYYMMDD>/<run_id>/<nama_file>`.
  Run terpisah memiliki prefix berbeda agar hasil uji sebelumnya tidak tertimpa.
- Connection Airflow yang dipakai adalah **`s3-artefact`**, sesuai implementasi
  `s3_artefact.py`. Isi Extra dengan `bucket`, endpoint/region sesuai lingkungan.
  Untuk lokal, connection telah diarahkan ke `http://s3-local:9000`, bucket `postingfile`.
- Adapter `dags/utils/hooks/S3HookOcbc.py` memakai provider AWS Airflow dan
  menyediakan `conn_extra` yang diperlukan oleh kode hasil transkripsi.
- `s3_artefact.py` tidak dimodifikasi. DAG memeriksa URL hasil upload karena
  metode existing menangkap exception sebagai warning; upload gagal harus
  menggagalkan task, bukan melanjutkan pengiriman file lokal.

Backup sebelum integrasi tersedia di
`backups/dag_postingfile.before_s3_20261026_232053.py.bak`.
Untuk existing, siapkan connection `s3-artefact` pada Airflow dan pastikan
provider AWS, boto3, polars, serta smart_open tersedia. Buat run baru setelah
perubahan DAG; XCom run lama belum memiliki field URL S3 untuk task pengiriman.

Validasi lokal integrasi S3: run `s3_results_integration_20261026` berhasil pada
ketiga task. Tiga objek (split Rintis, split PWC, POSTFLIN) ditemukan pada bucket
`postingfile`; POSTFLIN berisi lima transaksi dan byte hasil kirim SFTP sama dengan
objek S3. Pemeriksaan jalur upload gagal juga memastikan task menerima error.

Input `local_path` juga diarsipkan melalui `S3ArtefactHook` menggunakan
`input_s3_app = data_payment/acquiring/file`. File yang diambil dari FTP/SFTP
(termasuk ZIP asli) diunggah ke
`artefacts/data_payment/acquiring/file/<YYYYMMDD>/<run_id>/<nama_file>`.
File hasil ekstraksi yang cocok dengan mask pemrosesan juga diarsipkan.
XCom setiap file yang diproses menyertakan `source_s3_url`. Direktori `local_path`
tetap menjadi staging ekstraksi/parsing. Backup sebelum perubahan input:
`backups/dag_postingfile.before_s3_input_20261026_232501.py.bak`.

### DataFrame pada dag_postingfile

Query `dag_postingfile` sekarang meminta `as_dataframe=True` pada hook existing
PostgreSQL/Oracle. Hook menggunakan `polars.read_database` dan mengembalikan Polars
DataFrame; pemanggil DAG lain tetap memakai perilaku lama bila opsi tersebut tidak
diaktifkan. `base_sql.py` dan `s3_artefact.py` tidak diubah untuk integrasi ini.

Data merchant diakses melalui nama kolom (`AP_ON_ID_10`, `MER_ACCEPTOR_POINT_ID`,
`MERCHANT_NUMBER`), bukan indeks tuple. Enrichment ONUS memakai left join berdasarkan
RRN, menjaga urutan dan jumlah transaksi; duplikat enrichment memakai hasil pertama.
Enrichment yang tidak lengkap menghasilkan error dengan daftar RRN terkait.
Generator fixed-width menerima record bernama, bukan pasangan tuple record/MID.
Backup sebelum perubahan memiliki suffix `before_dataframe_20261027_071056.bak`.
