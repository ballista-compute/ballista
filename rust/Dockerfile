# Run build-docker-image.sh to build this Dockerfile!
FROM rustlang/rust:nightly

ADD temp /opt
ADD . /opt/ballista

RUN cd /opt/ballista && \
  cargo install --path .

EXPOSE 50051

CMD ["executor"]



