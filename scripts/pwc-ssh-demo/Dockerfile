FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server bash tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -s /bin/bash pwcdemo \
    && echo 'pwcdemo:demo-local-only' | chpasswd \
    && mkdir -p /run/sshd /pwrcard/home/usr/data /pwrcard/home/usr/responses \
    && chown pwcdemo:pwcdemo /pwrcard/home/usr/responses
COPY PWC.sh /opt/pwc/PWC.sh
RUN printf '\nPasswordAuthentication yes\nPermitRootLogin no\nAllowUsers pwcdemo\n' >> /etc/ssh/sshd_config
CMD ["/bin/bash", "-c", "ssh-keygen -A && exec /usr/sbin/sshd -D -e"]
