package com.nisum.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkSessionConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSessionConfig.class);

    @Bean
    public SparkSession sparkSession() {
        LOGGER.info("Initializing Spark Session Instance...using Config class for kafka-line-word ");
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("IntegrationtoMongo")
                .set("spark.sql.warehouse.dir", "D:\\tmp\\spark-session");

        return SparkSession.builder().config(sparkConf)   .getOrCreate();
    }
}