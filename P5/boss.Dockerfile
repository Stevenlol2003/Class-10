FROM p5-base
RUN wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && tar -xf spark-3.5.0-bin-hadoop3.tgz && rm spark-3.5.0-bin-hadoop3.tgz

CMD ["/spark-3.5.0-bin-hadoop3/sbin/start-master.sh", " sleep infinity"]



