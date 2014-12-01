package com.chiwanpark.example;

public class ExampleMain {
  public static void main(String... args) throws Exception {
    String algorithm = args[0].toLowerCase();
    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, args.length - 1);

    if ("pagerank".equals(algorithm)) {
      PageRankExample.main(subArgs);
    } else {
      throw new IllegalArgumentException("Given algorithm is not implemented yet.");
    }
  }
}
