# Apache Flink Example

This repository contains some example implementation of algorithm using Apache Flink ([http://flink.incubator.apache.org](http://flink.incubator.apache.org)). Must be compiled with Eclipse JDT compiler for using lambda expressions.

## Currently Implemented

* PageRank ([http://en.wikipedia.org/wiki/PageRank](http://en.wikipedia.org/wiki/PageRank))
    * Arguments - `pagerank <INPUT> <OUTPUT> <NUM_OF_VERTICES>`
    * Sample data - Twitter network data in [Stanford Large Network Dataset Collection](http://snap.stanford.edu/data/egonets-Twitter.html)

## How to Build and Run

1. Use `mvn package` command to build. This command will compile whole source code using Eclipse JDT Compiler.
2. Submit to Apache Flink cluster with proper arguments.
    * Example for PageRank - `./bin/flink run flink-example-1.0-SNAPSHOT.jar pagerank <INPUT> <OUTPUT> <NUM_OF_VERTICES>`
