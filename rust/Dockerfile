# Run build-rust-benchmarks.sh to build this rust-benchmarks.dockerfile!
FROM rustlang/rust:nightly

ADD temp /opt
ADD . /opt/ballista

RUN cd /opt/ballista && \
  cargo install --path .

EXPOSE 50051

CMD ["executor"]



