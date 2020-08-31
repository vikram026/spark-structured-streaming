package com.nisum.spark.jobs;

import com.google.common.collect.Lists;
import com.nisum.spark.model.Apple;
import com.nisum.spark.writer.ForEachWriterImpl;
import com.sun.media.jfxmedia.logging.Logger;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.orc.storage.ql.util.TimestampUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import scala.App;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class SaveToMongoService {
    @Autowired
    private  SparkSession sparkSession;

    public void runSparkJob() {
        log.info("Started Job Service");
        try {

            //producer();

            Dataset<Row> rowDataset = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "http://127.0.0.1:9092")
                    .option("subscribe", "sampleTopic")
                    .option("startingOffsets", "earliest")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

            rowDataset.printSchema();


            rowDataset.writeStream()
                .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/test")
				.outputMode(OutputMode.Append())
				.option("checkpointLocation", "/tmp/SparkSessionMongo")
				//.foreach(new ForEachWriterImpl())
				.start()
				.awaitTermination();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            sparkSession.close();
        }
        log.info("Ending the reading by the spark");
    }


        private void producer() throws InterruptedException, UnknownHostException {
            Properties config = new Properties();
            config.put("client.id", "client");
            config.put("bootstrap.servers", "127.0.0.1:9092");
            config.put("key.serializer", StringSerializer.class);
            config.put("value.serializer", KafkaAvroSerializer.class);
            config.put("acks", "all");
            KafkaProducer<String, Apple> kafkaProducer = new KafkaProducer<>(config);
            for(int i=0;i<100;i++) {
                Apple apple=new Apple("red",i*5);

                ProducerRecord<String, Apple> producerRecord = new ProducerRecord<>("sampleTest", "color", apple);
                Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
                log.info("Received MetaData: {}", future);
               // Thread.sleep(1000);

            }
    }

}
