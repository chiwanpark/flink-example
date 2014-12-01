package com.chiwanpark.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PageRankExample {
  private static final int MAX_ITERATION = 100000;
  private static final double ALPHA = 0.85;
  private static final double EPSILON = 0.0001;

  public static void main(String... args) throws Exception {
    final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    // read args
    String inputPath = args[0];
    String outputPath = args[1];
    int countOfVertexes = Integer.valueOf(args[2]);

    // create all edges
    DataSet<Tuple2<Long, Long>> edges = environment.readCsvFile(inputPath).fieldDelimiter(' ').lineDelimiter("\n").types(Long.class, Long.class);

    // extract vertices from edges
    DataSet<Long> vertices = edges.distinct(0).map(value -> value.f0);

    // create adjacency list from edges
    DataSet<Tuple2<Long, Long[]>> adjacencyList = edges.groupBy(0).reduceGroup((values, out) -> {
      List<Long> outgoings = new ArrayList<>();
      long u = -1;

      for (Tuple2<Long, Long> edge : values) {
        u = edge.f0;
        outgoings.add(edge.f1);
      }

      out.collect(new Tuple2<>(u, outgoings.toArray(new Long[outgoings.size()])));
    });

    // initial link table
    DataSet<Tuple2<Long, Double>> initialRanks = vertices.map(value -> new Tuple2<>(value, 1.0d / countOfVertexes));

    // iteration start
    IterativeDataSet<Tuple2<Long, Double>> iteration = initialRanks.iterate(MAX_ITERATION);

    DataSet<Tuple2<Long, Double>> distributedRanks = iteration.join(adjacencyList).where(0).equalTo(0).flatMap((value, out) -> {
      double currentRank = value.f0.f1;
      Long[] outgoings = value.f1.f1;
      double outgoingRank = currentRank / outgoings.length;

      for (Long outgoing : outgoings) {
        out.collect(new Tuple2<>(outgoing, outgoingRank));
      }
    });

    DataSet<Tuple2<Long, Double>> aggregatedRanks =
        distributedRanks.groupBy(0).aggregate(Aggregations.SUM, 1).map(value -> new Tuple2<>(value.f0, value.f1 * ALPHA + (1 - ALPHA) / countOfVertexes));

    // define terminate condition
    DataSet<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> terminatedCondition =
        aggregatedRanks.join(iteration).where(0).equalTo(0).filter(value -> Math.abs(value.f0.f1 - value.f1.f1) > EPSILON);

    // iteration end
    DataSet<Tuple2<Long, Double>> pageRanks = iteration.closeWith(aggregatedRanks, terminatedCondition);

    // print output
    pageRanks.writeAsCsv(outputPath, "\n", " ");

    environment.execute("Pagerank Algotirhm");
  }
}
