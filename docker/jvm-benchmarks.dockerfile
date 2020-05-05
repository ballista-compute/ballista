FROM openjdk:11

ADD build/distributions/benchmarks.tar /opt

EXPOSE 50051

ENV JAVA_OPTS="-Xms4g -Xmx8g"

CMD ["/opt/benchmarks/bin/benchmarks"]
