package com.nisum.spark.writer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.ForeachWriter;

import java.util.Properties;

@Slf4j
public class ForEachWriterImpl extends ForeachWriter<String> {

    private static final long serialVersionUID = 1L;
    private Properties properties = new Properties();
    private Producer<String, String> producer;
    private String topic="outputTopic2";
    private ProducerRecord<String, String> producerRecord;

    @Override
    public boolean open(long l, long l1) {
        log.info("into open method");
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "1");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return true;
    }

    @Override
    public void process(String word) {
        log.info("into process method :: "+word);

//        if(word.equalsIgnoreCase("error"))
//            throw new RuntimeException("We have error in process");

        try {
            producer = new KafkaProducer<String, String>(properties);
            producerRecord = new ProducerRecord<String, String>(topic, word);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("metadata : " + metadata);
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable throwable) {
        producer.close();
        producerRecord=null;

        System.out.println("spacific word Processed ");
    }
}