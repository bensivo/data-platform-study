FROM spark-base

WORKDIR /spark-worker

COPY ./spark-worker-entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

EXPOSE 8081

ENTRYPOINT ["/spark-worker/entrypoint.sh"]
