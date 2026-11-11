# Contoh merchant migration dari foto

File: `data/ACQ_MTI/ClearingRintis/PWC_OCBC_MERCHANT_20261111104150.txt`.

Berisi 18 baris: HD, dua kelompok MC/AC/ADR/COIDALL/COIDIPP/AP/AP/OCC,
dan TR. Encoding ASCII, newline LF, tanpa header kolom atau delimiter tambahan.
Nama file mengikuti tab pada foto. Header internal memakai tanggal 20260818
seperti yang terlihat pada foto, walaupun nama file memakai 20261111104150.
Dua merchant berakhiran 593 dan 592; masing-masing mempunyai AP Generic dan IPP.

Ini contoh rekonstruksi, bukan salinan byte-identik atau fixture tervalidasi BO.
Foto memperlihatkan beberapa posisi horizontal, tetapi tidak memberikan offset,
lebar record, atau seluruh digit nol yang dapat dipastikan. Padding spasi, blok
nol, sebagian kode, dan panjang trailer dibuat sebagai perkiraan untuk contoh.
Field yang terlihat seperti nama/alamat/contact disalin sebatas terbaca; bagian
yang terpotong tidak dapat dipulihkan secara pasti.

File dapat dipakai untuk mencoba transport streaming merchant_migration yang
memperlakukan isi sebagai byte. Untuk menguji keberhasilan import oleh
PWROCBC.sh, diperlukan file teks asli atau spesifikasi layout BO; contoh ini
belum dapat dinyatakan valid untuk parser tersebut.

Tidak ada perubahan DAG, connection, atau folder utils. Path input DAG tetap
mengikuti konfigurasi SFTP yang sudah ada; membuat file lokal ini tidak mengirim
atau mengeksekusinya pada server BO.
