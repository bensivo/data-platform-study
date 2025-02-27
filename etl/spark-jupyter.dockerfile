FROM spark-base

WORKDIR /spark-jupyter

RUN python3 -m venv /spark-jupyter/venv
RUN /spark-jupyter/venv/bin/pip install pyspark==3.5.3 jupyterlab

ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

ENV PYSPARK_DRIVER_PYTHON='/spark-jupyter/venv/bin/jupyter'
ENV PYSPARK_DRIVER_PYTHON_OPTS='lab --no-browser --ip=0.0.0.0 --port=8082 --allow-root'
ENV PYSPARK_PYTHON='/spark-jupyter/venv/bin/python'

EXPOSE 8082
ENTRYPOINT [ "/spark-jupyter/venv/bin/pyspark" ]