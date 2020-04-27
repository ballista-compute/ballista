FROM openjdk:11

ADD build/distributions/ballista-spark-*.tar /opt
RUN ln -s /opt/ballista-spark-* /opt/ballista-spark

EXPOSE 50051

CMD ["/opt/ballista-spark/bin/ballista-spark"]