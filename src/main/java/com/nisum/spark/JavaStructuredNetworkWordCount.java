//package com.nisum.spark;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.stereotype.Service;
//
//import java.util.Arrays;
//
///**
// * Counts words in UTF8 encoded, '\n' delimited text received from the network.
// *
// * Usage: JavaStructuredNetworkWordCount <hostname> <port>
// * <hostname> and <port> describe the TCP server that Structured Streaming
// * would connect to receive data.
// *
// * To run this on your local machine, you need to first run a Netcat server
// *    `$ nc -lk 9999`
// * and then run the example
// *    `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCount
// *    localhost 9999`
// */
//
//public  class JavaStructuredNetworkWordCount {
//
//  public static void main(String[] args) throws Exception {
//
//
//    String host = "localhost";
//    int port =9999;
//
//
//    SparkSession spark = SparkSession
//            .builder()
//            .appName("JavaStructuredNetworkWordCount")
//            .master("local[*]")
//            .getOrCreate();
//
//    // Create DataFrame representing the stream of input lines from connection to host:port
////    Dataset<Row> lines = spark
//////      .readStream()
//////      .format("socket")
//////      .option("host", host)
//////      .option("port", port)
//////      .load();
//
//
//
//    // Split the lines into words
//    Dataset<String> words = df.as(Encoders.STRING()).flatMap(
//        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
//        Encoders.STRING());
//
//    // Generate running word count
//    Dataset<Row> wordCounts = words.groupBy("value").count();
//
//    // Start running the query that prints the running counts to the console
//    StreamingQuery query = wordCounts.writeStream()
//      .outputMode("complete")
//      .format("console")
//      .start();
//
//    query.awaitTermination();
//  }
//}