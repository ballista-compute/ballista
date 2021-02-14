# Frequently Asked Questions

## What is the relationship between Apache Arrow, DataFusion, and Ballista?

Apache Arrow is a library which provides a standardized memory representation for columnar data. It also provides
"kernels" for performing common operations on this data.

DataFusion is a library for executing queries in-process using the Apache Arrow memory 
model and computational kernels. It is designed to run within a single process, using threads 
for parallel query execution. 

Ballista is a distributed compute platform design to leverage DataFusion and other query
execution libraries.