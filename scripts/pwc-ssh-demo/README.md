# Simulasi PWC melalui SSH

## Membuat container

Pastikan Docker Engine berjalan, Docker Compose tersedia, dan port lokal `2222`
belum digunakan. Jalankan dari root project:

```bash
cd /home/enigma_camp/myproject/airflow-project
mkdir -p data/pwrcard/home/usr/data
docker compose -f scripts/pwc-ssh-demo/compose.yaml up -d --build
docker compose -f scripts/pwc-ssh-demo/compose.yaml ps
```

Perintah `up -d --build` membangun image dari `Dockerfile`, menyalin script dummy
`PWC.sh` ke `/opt/pwc/PWC.sh`, lalu membuat dan menjalankan server SSH di background.
Build pertama membutuhkan akses internet untuk mengunduh image dan paket.
File input lokal dipasang read-only; response disimpan di volume `responses`.

Untuk melihat log atau masuk ke shell container:

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml logs --tail 50 pwc-ssh-demo
docker compose -f scripts/pwc-ssh-demo/compose.yaml exec pwc-ssh-demo bash
```

## Nama container

Tanpa `container_name`, Docker Compose membuat nama otomatis dengan pola
`<project>-<service>-<nomor>`. Pada konfigurasi ini, nama defaultnya adalah
`pwc-ssh-demo-pwc-ssh-demo-1`:

- Project `pwc-ssh-demo` berasal dari nama folder tempat file Compose berada.
- Service `pwc-ssh-demo` berasal dari key di bawah `services:`.
- Angka `1` adalah nomor instance container.

Nama project bisa berbeda jika memakai opsi `-p` atau `COMPOSE_PROJECT_NAME`.
Karena itu, gunakan perintah Compose dengan nama service seperti contoh di atas.

Opsional: jika ingin nama container tetap `pwc-ssh-demo`, tambahkan
`container_name` pada `scripts/pwc-ssh-demo/compose.yaml`:

```yaml
services:
  pwc-ssh-demo:
    container_name: pwc-ssh-demo
    build: .
    # Pertahankan konfigurasi ports dan volumes yang sudah ada.
```

Kemudian jalankan kembali `docker compose -f scripts/pwc-ssh-demo/compose.yaml up -d`.
Contoh ini hanya panduan; konfigurasi saat ini tetap memakai nama otomatis.

## Menguji script melalui SSH

Siapkan file hasil mapping di `data/pwrcard/home/usr/data/`. Ganti nama file pada
contoh berikut dengan file yang tersedia; `RESPONSE_demo.txt` harus belum ada.

Jalankan dari root project. Docker Compose dan client `ssh` diperlukan.

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml up -d --build
bash scripts/run_pwc_ssh_demo.sh POSTFLIN_20260911222210.txt RESPONSE_demo.txt
```

Host `127.0.0.1`, port `2222`, username `pwcdemo`, password `demo-local-only`.
Pengaturan client ada di bagian atas `scripts/run_pwc_ssh_demo.sh`.
Saat koneksi pertama, bandingkan fingerprint yang ditampilkan SSH dengan:

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml exec pwc-ssh-demo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Script dummy berada di `/opt/pwc/PWC.sh`. File input dari
`data/pwrcard/home/usr/data/` dipasang read-only di `/pwrcard/home/usr/data/`.
Response disimpan dalam volume Docker terpisah; gunakan nama baru untuk setiap percobaan.
Tanggal memakai hari ini di Asia/Jakarta. Tidak ada transaksi yang diposting.

Lihat response:

```bash
ssh -p 2222 pwcdemo@127.0.0.1 'cat /pwrcard/home/usr/responses/RESPONSE_demo.txt'
```

Hentikan server (response tetap tersimpan):

```bash
docker compose -f scripts/pwc-ssh-demo/compose.yaml down
```

Server ini terpisah dari SFTP Airflow yang sudah ada. Workflow Airflow dan
`run_pwc_posting.sh` tidak diubah. Kredensial demo hanya untuk simulasi lokal.
