# Frequently Asked Questions

## What is the relationship between DataFusion and Ballista?

DataFusion is a library for executing queries in-process using the Apache Arrow memory 
model and computational kernels. It is designed to run within a single process, using threads 
for parallel query execution. 

Ballista is a distributed compute platform design to leverage DataFusion and other query
execution libraries.