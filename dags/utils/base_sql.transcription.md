# Catatan penyalinan base_sql.py

Sumber: `C:\Users\Enigma Camp\Downloads\WhatsApp Unknown 2026-10-26 at 22.35.17.zip`, ZIP terbaru bertanggal 26 Oktober 2026. Seluruh 33 foto di dalam ZIP diperiksa; semuanya menampilkan bagian dari `dags/utils/base_sql.py`.

Kode disalin ke `base_sql.py`. Spasi, kutip, pemenggalan baris, dan sebagian komentar/docstring dirapikan. Tidak ada perubahan pada `oracle.py` atau DAG yang sudah ada.

Foto tambahan dari chat tanggal 26 Oktober 2026 melengkapi seluruh bagian yang sebelumnya ditandai hilang/buram. Nomor baris berikut mengacu pada kode di foto:

- 34–46: implementasi `__setconnection__` sudah disalin.
- 229–257: pemetaan dan pemilihan kolom sudah disalin dari foto yang lebih jelas.
- 354–360: setup koneksi `mssql_insert_rows` sudah disalin.
- 394–395: `.alias(c)` dan `for c in df.columns` sudah dikonfirmasi dan disalin.
- 445–456: alternatif parser datetime yang dikomentari sudah disalin.
- 489–492: parser tanggal untuk `%Y-%m-%d`, `%d/%m/%Y`, dan `%m/%d/%Y` sudah disalin.
- 524–544: kedua alternatif kode yang dikomentari sudah disalin.

Seluruh penanda `SOURCE_GAP` dan placeholder `NotImplementedError` dari transkripsi sebelumnya sudah dihapus.

Pemeriksaan: sintaks Python berhasil diparse dengan `ast.parse`. Eksekusi/import belum diverifikasi. Dependensi lokal `dags/utils/BaseDags/BaseDagsClient.py` dan `dags/utils/s3_artefact.py` tidak tersedia di proyek ini. Konstruktor `Connection()` dan `PoolProxiedConnection()` dipertahankan sesuai foto; penyalinan lengkap tidak berarti kode sudah siap dijalankan. Belum dilakukan perbaikan logika atau integrasi.

## Integrasi DAG postingfile

`dag_postingfile.py` sekarang memakai `BaseSQL.read_polar_with_exec` untuk keempat query. Hasil DataFrame dikonversi dengan `.rows()` agar sesuai dengan pemrosesan tuple di DAG. Jalur ini sudah disesuaikan dari transkripsi: koneksi menggunakan hook provider Airflow, parameter diteruskan ke driver, seluruh baris dibaca langsung ke DataFrame dengan tipe aslinya, serta cursor/koneksi ditutup saat berhasil maupun gagal. Dependensi S3 dimuat hanya saat diperlukan dan `ConfigClient` hanya diimpor untuk pemeriksaan tipe. Jalur `__setconnection__` lainnya masih mengikuti foto.

Validasi integrasi: sintaks berhasil; pemeriksaan menggunakan driver dan DataFrame tiruan mencakup penerusan parameter, banyak baris, hasil kosong/tanpa result set, rollback, dan penutupan resource. Eksekusi Airflow/database dan Polars asli belum diuji karena Polars tidak terpasang di interpreter lokal.

Koreksi integrasi: inisialisasi `db_way4 = PostgrestEtlHook()`, `db_source = OracleEtlHook()`, dan `db_enrichment = PostgrestEtlHook()` dipertahankan. `PostgrestEtlHook` di DAG adalah alias impor untuk kelas existing `PostgresEtlHook`. Kedua wrapper hook meneruskan `read_polar_with_exec` ke `BaseSQL` dan mengembalikan `.rows()`, sehingga antarmuka daftar tuple tetap sama bagi semua pemanggil.
