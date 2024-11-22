FROM spark-base

WORKDIR /spark-master

COPY ./spark-master-entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

EXPOSE 7077 8080
ENTRYPOINT ["/spark-master/entrypoint.sh"]
