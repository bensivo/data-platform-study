FROM spark-base

WORKDIR /spark-thrift

COPY ./spark-thrift-entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

EXPOSE 10000

ENTRYPOINT ["/spark-thrift/entrypoint.sh"]
