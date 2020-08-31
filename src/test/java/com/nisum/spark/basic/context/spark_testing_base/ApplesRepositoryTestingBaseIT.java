//package com.nisum.spark.basic.context.spark_testing_base;
//
//import com.holdenkarau.spark.testing.SharedJavaSparkContext;
//import com.nisum.spark.basic.repository.ApplesRepository;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
//import org.junit.jupiter.api.Test;
//
//import java.util.Arrays;
//
//import static org.junit.Assert.assertEquals;
//
//public class ApplesRepositoryTestingBaseIT extends SharedJavaSparkContext {
//
//    private final ApplesRepository repository = new ApplesRepository();
//
//    @Test
//    public void testMass_WhenTwoApples_ThenSumOfWeights() {
//        Integer[] weights = {120, 150};
//        long expected = Arrays.stream(weights).reduce(0, (a, v) -> a + v);
//        Dataset<Row> df = new SQLContext(jsc()).createDataset(Arrays.asList(weights), Encoders.INT()).toDF("weight");
//
//        long actual = repository.mass(df);
//
//        assertEquals(expected, actual);
//    }
//
//}
