FROM openjdk:11

ADD build/distributions/benchmarks.tar /opt

EXPOSE 50051

ENTRYPOINT ["/opt/benchmarks/bin/benchmarks"]