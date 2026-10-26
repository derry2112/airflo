# Catatan transkripsi s3_artefact.py

Sumber: foto kode yang dilampirkan di chat. Kode disalin ke `dags/utils/s3_artefact.py` dengan nama class `S3ArtefactHook`. Spasi, kutip, pemenggalan baris, dan beberapa docstring dirapikan; tidak dilakukan integrasi baru ke DAG.

Metode yang disalin: `__init__`, `stream_s3_to_ftp`, `stream_sftp_to_s3`, `put_file_to_s3`, `consume_file_in_s3_to_dataframe`, `read_large_csv_in_chunks`, `_inject_content_md5`, `housekeep`, `_delete_batch_safe`, `_delete_single_objects_concurrent`, `count_rows_s3`, dan `read_csv_fixed_width_to_dict`.

Pemeriksaan sintaks dengan `ast.parse` berhasil. Belum ada eksekusi upload, streaming, atau housekeeping. Tidak ada perubahan pada hook PostgreSQL/Oracle, konfigurasi, maupun DAG existing.

## Dependensi

- `dags/utils/hooks/S3HookOcbc.py` diimpor sesuai foto, tetapi file tersebut belum tersedia di proyek. Import `s3_artefact.py` belum dapat berhasil sampai hook ini dilengkapi.
- `dags/utils/hooks/SFTPHookOcbc.py` sudah tersedia.
- Kode menggunakan `polars`, `smart_open`, `boto3`, Airflow, serta `utils.telemetry.tracing`. Import tracer dipertahankan seperti foto.
- Komentar versi Airflow/Polars dan klaim kompatibilitas pada docstring disalin dari foto; bukan hasil verifikasi kompatibilitas lingkungan ini. Tidak ada perubahan requirements pada penyalinan ini.

## Perilaku yang dipertahankan dari foto

- `stream_s3_to_ftp` menggunakan SFTP meskipun nama metode/docstring menyebut FTP.
- `put_file_to_s3` default menghapus file lokal sesudah upload (`is_delete_immediately=True`), mengaktifkan log debug botocore, dan menangkap exception sebagai warning.
- Cabang upload banyak file menggunakan satu key yang sama dengan `replace=True`.
- `housekeep` menghapus objek lama sesuai retention; metode ini hanya disalin dan tidak dijalankan.
- Parser CSV melakukan pemotongan berdasarkan baris fisik; bukan implementasi tambahan untuk record CSV yang berisi newline di dalam kutip.
- `count_rows_s3` melewati header hanya jika URL berakhiran `.csv`.
- Docstring dan nama parameter yang tidak konsisten dipertahankan. Tidak ada perbaikan logika dalam tugas transkripsi ini.
