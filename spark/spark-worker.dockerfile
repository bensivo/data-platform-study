FROM spark-base

WORKDIR /spark-worker

COPY ./worker-entrypoint.sh worker-entrypoint.sh
RUN chmod +x worker-entrypoint.sh

EXPOSE 8081

ENTRYPOINT ["/spark-worker/worker-entrypoint.sh"]
