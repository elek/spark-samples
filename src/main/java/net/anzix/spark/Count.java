package net.anzix.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import java.util.concurrent.Callable;


@CommandLine.Command(name = "count")
public class Count implements Callable<Void> {

    @CommandLine.Parameters(index = "0", arity = "1")
    private String destination;

    @Override
    public Void call() throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Copy")
                .getOrCreate();

        final Dataset<Row> parquet = spark.read().parquet(destination);
        System.out.println(parquet.count());
        spark.stop();
        return null;
    }
}
