package UE3.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortByTempPartitioner extends Partitioner<DoubleWritable, DoubleWritable> {
    @Override
    public int getPartition(DoubleWritable doubleWritable1, DoubleWritable doubleWritable, int numPartitions) {
        return doubleWritable1.hashCode() % numPartitions;
    }
}