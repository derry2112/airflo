# Merchant migration (Airflow 3)

DAG `etl_MERCHANT_MIGRATION_PWC_SIT`, dijalankan manual
(`config['schedule_interval'] = None`), tanpa catchup. Tanggal mulai diambil dari
`config['start_date']` melalui `datetime.strptime` dengan `timezone(timedelta(hours=7))`,
mengikuti settlement/postingfile, tanpa pendulum.
Konfigurasi dan seluruh task ada dalam satu file
`dag_merchant_migration.py`; tidak ada file controller terpisah atau credential
dalam source. Struktur mengikuti dag_postingfile: konfigurasi di level modul,
fungsi @task dan helper di dalam fungsi @dag, tanpa class MerchantMigration.
Folder utils tidak diubah.

Connection yang dipakai: `postgres-db-acquiring` (control/acquiring),
`DB_MERCHANTAPPS` (Postgres enrichment), `oracle-db-sit-pwc` (PowerCARD),
`sftp-pten` (sumber MTI), `sftp-pwc` (BO), `ssh-pwc-bo` (eksekusi script),
`s3-artefact` (AWS, wajib extra `bucket` atau isi `s3_bucket` di config).
Pastikan endpoint SFTP/SSH BO mengarah filesystem yang sama dan source MTI
sesuai environment. Koneksi DB_MERCHANTAPPS mengikuti ID pada gambar;
ubah `merchantapps_conn_id` bila instalasi memakai ID lain.

Akses database memakai `PostgresEtlHook` dari `utils.postgres` dan
`OracleEtlHook` dari `dags.utils.oracle`, seperti pola DAG yang sudah ada.
Semua connection ID database berada di config: `acq_conn_id`,
`merchantapps_conn_id`, dan `pwc_conn_id`. Query Postgres memakai cursor hook
agar rowcount dan `INSERT ... RETURNING id` tetap tersedia. Lookup Oracle
memakai `read_polar_with_exec`; cleanup mengirim seluruh DELETE berurutan
dalam satu blok PL/SQL agar tetap menjadi satu transaksi.

File distream dari SFTP sumber ke S3, lalu dari S3 ke SFTP BO. Logic copy ke parent
juga melalui stream BO → S3 → BO, menggantikan `cp`. Tidak memakai file lokal;
XCom hanya berisi nama file. Object S3 dipisah per workflow/run_id, dan dipertahankan
sebagai artefak (atur lifecycle bucket sesuai kebutuhan).

## Path

Konfigurasi mempertahankan path dari gambar:
- source_path: `/mtiuat/toocbc/powercard/migration`
- destination_path: `/pwrcard/home/usr/data/merchant_migration`
- success_folder: `/pwrcard/home/usr/data/merchant_migration/process_success`
- failed_folder: `/pwrcard/home/usr/data/merchant_migration/process_failed`
- inprogress_folder: `/pwrcard/home/usr/data/merchant_migration/inprogress`
- local_path: `/data/airflow/nfs/artifacts/data_payment/acquiring/merchant_migration/`

Tiga folder status diteruskan ke fungsi proses, sesuai signature asal. Fungsi
tersebut belum memindahkan file ke folder status, mengikuti kode pada gambar.
local_path dipertahankan dan diteruskan sebagai konfigurasi kompatibilitas;
transfer tetap melalui S3, tanpa membuat staging file lokal. Path remote dirangkai
dengan `rstrip`/`rsplit`, tanpa posixpath.

## Batas sumber dan asumsi

Gambar tidak berisi BaseDags_MerchantMigration (dependency task) dan body
put_files_to_server_bo baris 52–76. Sesuai arahan user, struktur mengikuti DAG
TaskFlow di folder payment-and-collection-platform. Urutan aktif sekarang:
ambil → kirim BO → hapus sumber → ambil file control sukses → update enrichment.
Sesuai instruksi, `process_files_remote` disimpan sebagai fungsi biasa tanpa
`@task` dan tidak dipanggil pada wiring DAG. Helper SSH, copy-parent, dan logging
tetap disimpan sebagai logic. Script BO tidak dieksekusi oleh DAG ini.
Polling database hanya mengambil file yang sebelumnya sudah tercatat
`bash_status = 'SUCCESS'`; upload baru tidak dianggap sebagai migrasi sukses.
Method upload dikonstruksi dari kontrak input/output yang terlihat.
Urutan ini belum dapat diklaim identik dengan dependency DAG asli yang tidak terlihat.

SQL/status/urutan delete mengikuti gambar, termasuk:
- exit code 0 dan 26 dianggap sukses;
- enrichment 03 → 04 sukses atau 02 gagal;
- merchant parsial dibersihkan dalam satu transaksi Oracle;
- tidak ada pemindahan success/failed/inprogress karena method proses pada gambar
  tidak memanggil move_file;
- file dengan MID in-progress diproses tanpa mark_db_updated pada akhir loop,
  mengikuti gambar; mark hanya pada kasus tidak ada MID;
- filter filename pada update gagal tetap `filename`, sedangkan lookup in-progress
  memakai `file_name_pwc`, sesuai gambar.

Script dipertahankan: `./PWROCBC.sh ocbc_acquiring_migration <ddmmyyyy> <filename>
M NULL NULL NULL NULL NULL NULL NULL NULL`. Verifikasi jumlah argumen NULL dengan
script BO asli sebelum menjalankan integrasi. Tabel aplikasi/script BO tidak dibuat
oleh DAG ini. Import PowerCARD lewat SSH belum diuji.

## Pengujian lokal

Runner `scripts/test_merchant_migration_local.py` memakai koneksi SFTP/S3 lokal
yang telah ada dengan override config hanya di proses uji. File contoh asli
disalin ke folder staging SFTP terpisah. Hasil uji memeriksa SHA256 file asli,
object S3, dan file tujuan, serta penghapusan salinan sumber. File asli tidak dihapus.
Runner menolak endpoint nonlokal dan memblokir eksekusi SSH BO.
Laporan JSON disimpan di `data/artifacts/merchant_migration-local/<run>/report.json`.
