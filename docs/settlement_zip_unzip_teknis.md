# Istilah teknis penanganan ZIP dan unzip settlement

Pendekatan yang digunakan disebut **file ingestion pipeline dengan end-to-end
integrity verification dan fault isolation**: alur penerimaan file yang memeriksa
keutuhan data pada setiap perpindahan serta mengisolasi file yang gagal.
Istilah ini menjelaskan gabungan mekanisme dalam DAG, bukan nama satu algoritma
atau framework tertentu.

Untuk proses ZIP/unzip secara khusus, istilah yang tepat adalah **archive
validation and selective extraction**: memvalidasi arsip ZIP, lalu mengekstrak
hanya file yang memenuhi pola nama yang ditentukan.

## Pemetaan istilah dengan implementasi

| Istilah teknis | Makna | Implementasi pada DAG |
| --- | --- | --- |
| Archive validation | Pemeriksaan apakah arsip valid dan dapat dibaca | Memeriksa struktur ZIP dan CRC seluruh member sebelum ekstraksi. |
| CRC validation | Pemeriksaan kerusakan data di dalam arsip | CRC bawaan ZIP diverifikasi saat member dibaca; ZIP/member yang rusak ditolak. CRC bukan bukti keaslian pengirim. |
| Selective extraction | Ekstraksi hanya isi arsip yang dibutuhkan | Hanya member dengan nama `*D4_NPG.txt` yang diambil, tanpa membedakan huruf besar/kecil. |
| Path traversal prevention | Mencegah path dalam arsip menentukan lokasi penulisan di luar folder tujuan | Member ditulis menggunakan basename ke direktori sementara; basename D4 duplikat ditolak. |
| Integrity verification | Pemeriksaan bahwa isi file tidak berubah selama proses | Ukuran dan SHA-256 dibandingkan pada upload/download S3; ukuran TXT hasil unzip dibandingkan dengan metadata ZIP. |
| Source stability check | Pemeriksaan apakah sumber berubah saat sedang diunduh | Ukuran dan modification time SFTP diperiksa sebelum/sesudah download. Fallback FTP memeriksa ukuran. |
| Bounded retry with exponential backoff | Percobaan ulang terbatas untuk gangguan sementara dengan jeda bertambah | Maksimal 3 kali; jeda default 2 lalu 4 detik. Error format, CRC, checksum, dan rekonsiliasi langsung ditolak. |
| File quarantine | Pemisahan file bermasalah untuk pemeriksaan dan pemrosesan ulang | Bytes lokal yang tersedia dan `reason.json` disimpan pada prefix karantina di S3. Bila bytes tidak tersedia, manifest menyimpan referensi sumber. |
| Fault isolation | Membatasi dampak kegagalan pada unit file terkait | File bermasalah dilewati sementara file independen yang valid tetap diproses. CRC ZIP yang gagal menolak seluruh arsip. |
| Strict decoding and parsing | Pembacaan teks dan transaksi tanpa membuang error secara diam-diam | Encoding `utf-8-sig` menggunakan mode strict; transaksi dua baris yang tidak lengkap ditolak. |
| Atomic transaction per file | Seluruh perubahan database untuk satu file berhasil bersama atau dibatalkan bersama | Insert log, summary, dan detail memakai satu transaksi; kegagalan menyebabkan rollback. |
| Idempotent processing | Menjalankan ulang input yang sama tanpa menambah hasil duplikat | Pemeriksaan nama file pada log dan advisory transaction lock mencegah insert ulang oleh flow ini. |
| Operational observability | Informasi untuk mengetahui lokasi, tahap, dan penyebab kegagalan | Log `FILE RESULT`/`FILE REJECTED`, status XCom, serta manifest karantina. Task terakhir gagal jika ada file ditolak. |

## Contoh penjelasan kepada leader

> Untuk file settlement, kita menerapkan file ingestion pipeline dengan integrity
> verification. ZIP diperiksa struktur dan CRC-nya, lalu hanya TXT D4 yang
> diekstrak. Keutuhan file saat melewati S3 diverifikasi menggunakan ukuran dan
> SHA-256. Jika transfer gagal, sistem melakukan bounded retry maksimal tiga
> kali. File yang tetap bermasalah masuk karantina bersama catatan penyebabnya,
> sementara file valid tetap diproses melalui fault isolation. Penyimpanan ke
> database bersifat atomic per file dan idempotent, sehingga kegagalan tidak
> meninggalkan data sebagian dan eksekusi ulang tidak membuat duplikasi.

Versi singkat untuk rapat:

> Penanganan ZIP/unzip menggunakan archive validation, selective extraction,
> checksum verification, retry, dan file quarantine. Data yang lolos validasi
> diproses dengan transaksi database atomic per file dan proteksi duplikasi.

## Batas jaminan yang perlu dijelaskan

- Checksum memastikan byte hasil transfer cocok dengan pembanding yang dibuat
  setelah download sumber. Checksum ini bukan tanda tangan digital dan belum
  membuktikan keaslian atau kelengkapan data dari pengirim.
- Jika transaksi lengkap sudah hilang sebelum ZIP dibuat tetapi struktur file
  masih valid, perlu checksum/jumlah transaksi/trailer tepercaya dari pengirim.
  Footer SALES yang tersedia kini divalidasi; bagian tanpa footer dicatat
  sebagai belum terkonfirmasi kelengkapannya.
- Retry membantu mengatasi gangguan sementara; retry tidak memperbaiki data
  sumber yang memang rusak atau melengkapi transaksi yang hilang.
- Atomicity berlaku per file D4, bukan seluruh batch. File valid dapat sudah
  commit meskipun status akhir DAG gagal karena file lain dikarantina.
- Proteksi duplikasi menggunakan identitas nama file dan lock yang diikuti flow
  ini. Koreksi file dengan nama yang sudah sukses memerlukan prosedur
  reprocessing; writer lain harus mengikuti mekanisme lock yang sama.
- Implementasi ini tidak mencakup pembuatan ZIP sumber, antivirus, atau jaminan
  perlindungan terhadap semua jenis arsip berbahaya seperti ZIP bomb.

Referensi implementasi:
[`dag_settlement_off_us.py`](../dags/payment-and-collection-platform/acquiring/dag_settlement_off_us.py).

Saya sesuaikan pengecekannya menjadi ukuran dan waktu perubahan sumber yang stabil, CRC ZIP, checksum selama transfer, serta pencocokan jumlah/nominal dengan footer laporan jika tersedia. Audit, retry bertahap, dan kontrol koreksi tetap saya lanjutkan. Tanpa pembanding dari pengirim, kelengkapan file sumber memang tidak bisa dijamin hanya dari ZIP yang valid.

## Implementasi kontrol operasional

Flow aktif tetap menerima ZIP saja. Penanda `.done` dan manifest pengirim
tidak diwajibkan. Pemeriksaan kestabilan sumber, rekonsiliasi footer, audit
permanen, unique index, dan koreksi dengan versi/persetujuan dijelaskan di
[settlement_ingestion_operasional.md](settlement_ingestion_operasional.md).

| Mekanisme | Penerapan yang disarankan |
|---|---|
| **File readiness** | Pengirim mengunggah sebagai `.part`, lalu mengganti nama menjadi `.zip` setelah selesai, atau menyediakan file penanda `.done`. DAG hanya mengambil file yang sudah siap. |
| **Manifest dari pengirim** | Sertakan nama file, ukuran, SHA-256, jumlah transaksi, dan total nominal. Ini menjadi pembanding independen untuk mengetahui kelengkapan sumber. |
| **Business reconciliation** | Cocokkan jumlah transaksi dan total nominal hasil parsing dengan manifest atau trailer laporan sebelum insert. |
| **Klasifikasi error** | Gangguan jaringan di-retry dengan jeda bertambah (*exponential backoff*). Format salah atau transaksi terpotong langsung dikarantina. |
| **Audit status permanen** | Simpan status `RECEIVED → VERIFIED → VALIDATED → LOADED`, atau `QUARANTINED`, pada tabel kontrol beserta checksum dan penyebab kegagalan. |
| **Proteksi duplikasi di database** | Tambahkan unique constraint pada identitas file yang disepakati, melengkapi pemeriksaan aplikasi dan lock. |
| **Pemrosesan ulang terkontrol** | File koreksi memiliki versi dan jejak persetujuan; operator tidak perlu menghapus data secara manual. |