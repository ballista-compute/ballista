FROM openjdk:11

ADD build/distributions/executor.tar /opt

EXPOSE 50051

CMD ["/opt/executor/bin/executor"]