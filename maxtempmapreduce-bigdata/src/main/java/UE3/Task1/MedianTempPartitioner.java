package UE3.Task1;

import UE3.TemperaturePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MedianTempPartitioner extends Partitioner<TemperaturePair, DoubleWritable> {
    @Override
        public int getPartition(TemperaturePair temperaturePair, DoubleWritable doubleWritable, int numPartitions) {
            return temperaturePair.getYearMonth().hashCode() % numPartitions;
        }
    }

