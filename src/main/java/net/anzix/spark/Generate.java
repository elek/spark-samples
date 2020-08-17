package net.anzix.spark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Hello world!
 */
@CommandLine.Command(name = "generate")
public class Generate implements Callable<Void> {


    @CommandLine.Parameters(index = "0", arity = "1")
    private String destination;

    @CommandLine.Option(names = "--record-size", defaultValue = "1048576")
    private int size;

    @CommandLine.Option(names = "--records", defaultValue = "100")
    private int records;

    @CommandLine.Option(names = "--iteration", defaultValue = "1")
    private int iteration;

    private static void initializeParquet(String destination, SparkSession spark) {
        List<Data> values = new ArrayList<>();
        values.add(0, new Data(0, new byte[]{0}));
        Dataset<Row> df = spark.createDataFrame(values, Data.class);
        df.write().parquet(destination);
    }

    @Override
    public Void call() throws Exception {

        StringBuilder sb = new StringBuilder();

        String testData = destination;

        SparkSession spark = SparkSession.builder()
                .appName("Generate").getOrCreate();

        initializeParquet(testData, spark);

        for (int j = 0; j < iteration; j++) {
            List<Data> values = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                final byte[] bytes = RandomStringUtils.randomAscii(size)
                        .getBytes(StandardCharsets.UTF_8);
                values.add(new Data(j * iteration + i,
                        bytes));
            }
            Dataset<Row> df = spark.createDataFrame(values, Data.class);
            df.write().mode(SaveMode.Append).parquet(testData);
        }

        spark.stop();
        return null;
    }

    public static class Data {
        private int index;
        private byte[] data;

        public Data(int index, byte[] data) {
            this.index = index;
            this.data = data;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }
    }
}
