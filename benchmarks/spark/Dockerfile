FROM gradle:latest

RUN mkdir /benchmarks
RUN mkdir /benchmarks/src
WORKDIR /benchmarks

COPY src ./src
COPY settings.gradle .
COPY build.gradle .

RUN gradle assemble

FROM openjdk:8
COPY --from=0 /benchmarks/build/distributions/spark-benchmarks.tar .
RUN tar xf spark-benchmarks.tar
WORKDIR /spark-benchmarks

# Make JVM respect CPU and RAM limits within the container
ENV SPARK_BENCHMARKS_OPTS "-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"

VOLUME /mnt/nyctaxi




