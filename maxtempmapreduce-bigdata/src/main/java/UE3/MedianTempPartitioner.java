package UE3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MedianTempPartitioner extends Partitioner<TemperaturePair, DoubleWritable> {
    @Override
        public int getPartition(TemperaturePair temperaturePair, DoubleWritable doubleWritable, int numPartitions) {
            return temperaturePair.getYearMonth().hashCode() % numPartitions;
        }
    }

