package UE3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MedianTempPartitioner extends Partitioner<TemperaturePair, NullWritable> {
    @Override
    public int getPartition(TemperaturePair temperaturePair, NullWritable nullWritable, int i) {
        return temperaturePair.getYearMonth().hashCode() % i;
    }
}
