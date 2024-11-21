FROM spark-base

WORKDIR /spark-master

COPY ./master-entrypoint.sh master-entrypoint.sh
RUN chmod +x master-entrypoint.sh

EXPOSE 7077 8080
ENTRYPOINT ["/spark-master/master-entrypoint.sh"]
