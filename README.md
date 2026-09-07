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