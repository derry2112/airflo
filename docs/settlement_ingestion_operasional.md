# Kontrol ingestion settlement: operasional dan deployment

## Kontrak yang aktif: hanya ZIP

Pengirim saat ini hanya menyediakan ZIP. Karena itu
`require_sender_manifest=False` menjadi default dan tidak ada kewajiban file
`.done` atau manifest pendamping. DAG mengambil ZIP hari ini (`YYMMDD`, WIB).

Kesiapan file diperiksa dengan dua snapshot ukuran/mtime yang berselang
`source_stability_seconds=5`. Untuk SFTP, file juga harus sudah berumur minimal
`source_min_age_seconds=30`. File yang masih berubah, terlalu baru, atau kosong
mendapat audit `WAITING_READY` dan ditunda hingga run berikutnya. Fallback FTP
hanya memeriksa stabilitas ukuran karena mtime tidak digunakan.

Ini adalah pemeriksaan berbasis kestabilan metadata, bukan jaminan pengirim telah
selesai. Ukuran/mtime diperiksa lagi sebelum dan sesudah download. CRC seluruh
ZIP diperiksa; ukuran dan SHA-256 melindungi perpindahan ZIP/TXT melalui S3.
Upload S3 memakai `S3ArtefactHook.put_file_to_s3` dari `utils/s3_artefact.py`.
DAG memeriksa URL hasilnya dan membaca ulang objek untuk membandingkan ukuran
serta SHA-256 seluruh file. File `utils` tidak diubah. Utility dapat menangkap
exception dan mengembalikan URL kosong; kasus ini diperlakukan sebagai kegagalan
transfer yang penyebabnya belum diketahui dan dicoba ulang secara terbatas.
Karena exception asli tidak diteruskan, klasifikasi error upload tersebut lebih
terbatas; penyebab asli perlu dilihat di log utility.
File yang sudah salah dari sumber tetap memerlukan konfirmasi pengirim.

## Rekonsiliasi data

Footer `0 SALES <jumlah> <nominal>` yang tersedia dicocokkan dengan transaksi
sejak footer sebelumnya pada bank/periode yang sama, termasuk halaman lanjutan.
Jumlah atau nominal yang tidak cocok menolak seluruh file D4 sebelum insert.

Jika sebuah bagian tidak memiliki footer lengkap, DAG tetap menerima bagian
tersebut sesuai kontrak ZIP saat ini, tetapi mencatat `SOURCE COMPLETENESS
UNCONFIRMED` serta `banks_without_complete_footer` di audit. Dengan demikian,
status lolos validasi tidak boleh diartikan sebagai bukti kelengkapan sumber
untuk bagian tanpa footer. File contoh memiliki bagian BCA 2023-09-25 tanpa
footer; bagian ini akan tercatat sebagai belum terkonfirmasi.

Mode manifest pengirim tersedia untuk kontrak masa depan, tetapi dinonaktifkan.
Jika disepakati, set `require_sender_manifest=True`; pengirim harus menerbitkan
`<ZIP>.manifest.json` dan `<ZIP>.done` (penanda diterbitkan terakhir). Manifest
versi 1 memuat `archive: {filename, size, sha256}` dan `files: [...]`; setiap
file D4 memiliki `filename`, `size`, `sha256`, `transaction_count`, `total_amount`,
dan `banks` berisi `acquirer`, `period` (YYYY-MM-DD), `transaction_count`, serta
`total_amount`. Nominal berupa string dua desimal. Semua nama D4, checksum,
jumlah, dan nominal bank/periode harus cocok. Manifest yang dihitung sendiri
oleh penerima tidak menjadi pembanding independen dari pengirim.

## Retry berdasarkan jenis kegagalan

Maksimal tiga percobaan, memakai exponential backoff: jeda 2 detik lalu 4 detik
(default, batas jeda 30 detik jika jumlah percobaan ditambah).

- Timeout, koneksi putus, dan error layanan sementara seperti HTTP 429/5xx
  dapat dicoba kembali.
- Exception izin ditolak atau file hilang yang diteruskan utility tidak diulang;
  jika exception ditelan dan hanya URL kosong dikembalikan, berlaku retry terbatas
  sebagaimana dijelaskan di atas.
- ZIP/CRC rusak, SHA-256 berbeda, rekonsiliasi gagal, format/encoding salah,
  file tidak ditemukan, atau izin ditolak tidak diulang otomatis.
- Insert database tidak masuk mekanisme retry transfer. Error database membatalkan
  seluruh transaksi file dan diteruskan ke penanganan kegagalan.

Karantina S3 tetap menyimpan bytes yang tersedia dan `reason.json`. File valid
lain diproses, lalu task akhir menggagalkan run bila ada file ditolak. Jika S3
karantina gagal, sistem berusaha mencatat kegagalan di audit database. Jika
keduanya tidak tersedia, log/XCom mencatat `quarantine_failed`.

## Migrasi database

Untuk instalasi baru, jalankan `dags/sql/rekon.sql`, kemudian migrasi tambahan:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 \
  -U airflow -d db_rekon_uat < dags/sql/settlement_ingestion_controls.sql
```

Migrasi tambahan dapat dijalankan ulang. Migrasi menambah tabel audit dan
persetujuan koreksi serta kolom checksum, versi, `isactive`, dan `replacesfileid`
pada log. Unique index menegakkan satu versi D4 aktif per nama file (tanpa
membedakan kapitalisasi), serta keunikan nomor versi per nama file. Jika terdapat
duplikat lama, migrasi gagal; data tidak dihapus untuk memaksakan constraint.

Data lama yang belum memiliki checksum **tidak diisi checksum secara otomatis**.
Eksekusi ulang nama tersebut ditolak untuk review operator. Operator perlu
memverifikasi arsip sumber yang pernah dimuat dan kecocokannya dengan detail
tersimpan sebelum menetapkan baseline checksum lewat prosedur migrasi data
terkontrol. Jangan menggunakan checksum file baru begitu saja untuk mengisi log
lama. Hal ini bukan alasan untuk menghapus riwayat settlement.

## Audit permanen

Tabel `sw_rekon.acqpwc_ingestion_audit_tt` menyimpan event berurutan per file/run:

`RECEIVED → VERIFIED → VALIDATED → LOADED`

Status lain: `WAITING_READY`, `ALREADY_PROCESSED`, `QUARANTINED`, dan
`QUARANTINE_FAILED`. ZIP dan D4 memiliki event masing-masing; `zipfilename`
menghubungkan D4 dengan ZIP asal. `LOADED` dicatat dalam transaksi yang sama
dengan log, summary, dan detail. Event tahap sebelumnya tetap tersimpan jika
insert kemudian gagal. Detail audit memuat hasil pemeriksaan footer.

```sql
SELECT auditid, runid, filename, zipfilename, status, stage, details, createdat
FROM sw_rekon.acqpwc_ingestion_audit_tt
ORDER BY auditid DESC
LIMIT 100;
```

## Koreksi dengan versi dan persetujuan

- Nama dan SHA-256 sama dengan versi aktif: skip sebagai `ALREADY_PROCESSED`.
- Nama sama tetapi isi berbeda: ditolak kecuali ada request `APPROVED` yang
  tepat mengikat checksum versi aktif dan checksum koreksi.
- Nama sama dengan checksum lama belum diketahui: review baseline diperlukan.
- Koreksi disetujui: versi sebelumnya dinonaktifkan, versi berikutnya dimuat,
  dan request ditandai `APPLIED` dalam satu transaksi. Detail lama tetap ada.
  Jika insert gagal, versi lama tetap aktif dan request tetap `APPROVED`.

Operator yang berwenang dapat memakai
[`settlement_approve_reprocess.sql`](../dags/sql/settlement_approve_reprocess.sql)
setelah memeriksa file koreksi serta tiket/alasan. Script mencatat identitas
`current_user` PostgreSQL sebagai pemberi persetujuan. DAG tidak membuat request
persetujuan secara otomatis.

Untuk produksi, DBA harus memisahkan role operator pemberi persetujuan dan role
runtime DAG. Runtime tidak boleh mendapat izin INSERT/DELETE pada tabel request
atau izin mengubah checksum/identitas pemberi persetujuan; hanya baca request dan
update kolom penerapan yang diperlukan. Audit digunakan append-only oleh aplikasi;
batasi izin UPDATE/DELETE untuk runtime. Akun demo lokal adalah akun dengan hak
luas, sehingga bukan contoh pemisahan otorisasi produksi. Migrasi ini tidak membuat
akun atau menetapkan role organisasi secara otomatis.

Query bisnis harus memilih versi aktif agar versi historis tidak ikut dijumlahkan:

```sql
SELECT f.filename, f.fileversion, s.issuer, s.period, s.frekuensi, s.totalamount
FROM sw_rekon.acqpwc_file_log_tt f
JOIN sw_rekon.acqpwc_d4summary_tt s ON s.fileid=f.fileid
WHERE f.transactiontype='D4' AND f.isactive
ORDER BY f.fileid, s.summaryid;
```

## Pengujian

Unit dan simulasi pipeline:

```bash
python3 -m unittest discover -s tests -p 'test_settlement_d4.py' -v
```

Integrasi PostgreSQL memakai skema sementara yang dibersihkan pada akhir uji:

```bash
docker compose exec -T airflow python - < tests/verify_settlement_postgres.py
```

Uji integrasi tidak memakai tabel `sw_rekon`; mencakup insert, audit, duplikasi,
unique index, koreksi tanpa/dengan persetujuan, dan rollback termasuk kegagalan
saat menerapkan koreksi. Pengujian ini tidak memberikan persetujuan koreksi pada
data settlement nyata.
