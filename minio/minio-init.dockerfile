FROM debian:12

WORKDIR /minio-init

# Install base packages
RUN apt update
RUN apt install -y curl wait-for-it

# Install MinIO client into the /mino-init/bin directory
RUN curl https://dl.min.io/client/mc/release/linux-arm64/mc \
  --create-dirs \
  -o /minio-init/bin/mc
RUN chmod +x /minio-init/bin/mc

# Install our init script into the /minio-init/bin directory
COPY ./minio-init.sh /minio-init/bin/minio-init.sh
RUN chmod +x /minio-init/bin/minio-init.sh

ENV PATH=$PATH:/minio-init/bin

ENTRYPOINT ["/minio-init/bin/minio-init.sh"]
