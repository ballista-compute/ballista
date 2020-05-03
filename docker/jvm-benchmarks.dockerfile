FROM openjdk:11

ADD build/distributions/benchmarks.tar /opt

EXPOSE 50051

CMD ["/opt/benchmarks/bin/benchmarks"]