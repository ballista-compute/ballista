FROM ballistacompute/spark:3.0.0
USER root
ADD benchmarks/build/distributions/benchmarks.tar /opt/
ENTRYPOINT [ "/opt/benchmarks/bin/benchmarks" ]
CMD [ "--help" ]