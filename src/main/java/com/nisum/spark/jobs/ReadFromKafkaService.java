package com.nisum.spark.jobs;

import com.nisum.spark.writer.ForEachWriterImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;


@Service
@Slf4j
public class ReadFromKafkaService implements Serializable {

    @Autowired
    private SparkSession sparkSession;




    public void runSparkJob() {
        log.info("Started Job Service");
        try {
            Dataset<String> inputLines = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "http://127.0.0.1:9092")
                    .option("subscribe", "sampleTopic")
                    .option("startingOffsets", "earliest")
                    .load()
                    .selectExpr("CAST(value AS STRING)").as(Encoders.STRING());
            inputLines.printSchema();

           Dataset<String> inputWords = inputLines.flatMap(new TestMapper(), Encoders.STRING()).as(Encoders.STRING());


//
//            Dataset<String> inputWords = inputLines.flatMap(
//                    (FlatMapFunction<String, String>) x -> this.cartesianProduct( ()->Stream.of("s1", "s2","s3"),()->Stream.of("le1", "le2","le3")), Encoders.STRING()).as(Encoders.STRING());

//            while(inputWords.hasNext()){
//                log.info(inputWords.next());
//
//            }

            inputWords.writeStream()
                    .foreach(new ForEachWriterImpl())
                    .outputMode(OutputMode.Append())
                    .option("checkpointLocation", "D:\\tmp\\poc-output2")
                    .start()
                    .awaitTermination();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            sparkSession.close();
        }
        log.info("Ends of Spark Job");
    }

    private Iterator<String> cartesianProduct(Supplier<Stream<String>> lookupEntties, Supplier<Stream<String>> stores)  {
        return lookupEntties.get().flatMap(le -> stores.get().map(s-> le+s)).peek(e -> System.out.println("Filtered value: " + e)).iterator();
    }

}
