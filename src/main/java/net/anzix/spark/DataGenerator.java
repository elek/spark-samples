package net.anzix.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class DataGenerator extends Receiver<String> {

    public DataGenerator() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        new Thread(this::generate).start();
    }

    private void generate() {
        for (int i = 0; i < 100; i++) {
            store("" + i);
        }
        stop("Stream is ended");
    }

    @Override
    public void onStop() {

    }
}
