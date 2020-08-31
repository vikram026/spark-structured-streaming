package com.nisum.spark;

import com.nisum.spark.config.SparkSessionConfig;
import com.nisum.spark.jobs.ReadFromKafkaService;
import com.nisum.spark.jobs.SaveToMongoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class SparkStructureStreamingApplication  implements CommandLineRunner {

	@Autowired
	private ReadFromKafkaService readFromKafkaService;

	@Autowired
	private  SaveToMongoService saveToMongoService;


	public static void main(String[] args) {
		SpringApplication.run(SparkStructureStreamingApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		log.info("Started Command Line Runner ");

		readFromKafkaService.runSparkJob();
		//saveToMongoService.runSparkJob();
	}


}
