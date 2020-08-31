package com.nisum.spark.basic.comparison;


import com.google.common.collect.Lists;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.nisum.spark.basic.model.Apple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ComparisonTest {

    //test the schema is matching or not;
    @Test
    public void testStructure() {
        List<Apple> data = Lists.newArrayList(new Apple("Green", 85));
        Dataset<Row> df = spark().createDataFrame(data, Apple.class);

        assertEquals(Encoders.bean(Apple.class).schema(), df.schema());
    }

    public SQLContext spark() {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("testjava")
                .set("spark.sql.warehouse.dir", "/tmp/SparktestSessionbytst");

        SparkSession sparkSession= SparkSession.builder().config(sparkConf).getOrCreate();
        return new org.apache.spark.sql.SQLContext(sparkSession);

    }

    //
    @Test
    public void testOneValue() {
        Apple apple = new Apple("Green", 85);
        Apple apple1 = new Apple("Green", 84);

        List<Apple> data = Lists.newArrayList(apple,apple1);
        Dataset<Row> df = spark().createDataFrame(data, Apple.class);
        Integer actual = df.first().getAs("weight");
        assertEquals(apple.getWeight(), actual);
    }

    @Test
    public void testPrimitivesList() {
        List<String> data = Lists.newArrayList("green", "red");
        Dataset<Row> df = spark().createDataset(data, Encoders.STRING()).toDF("color");
        List<String> actual = df.select("color").as(Encoders.STRING()).collectAsList();
        assertEquals(actual.size(), 2);
    }

    @Test
    public void testDataFrames() {

        List<Apple> data = Lists.newArrayList(new Apple("Green", 85));
        Dataset<Row> expected = spark().createDataFrame(data, Apple.class);
        Dataset<Row> actual = spark().createDataFrame(data, Apple.class);
        assertEquals(0, expected.except(actual).count());

    }
}
