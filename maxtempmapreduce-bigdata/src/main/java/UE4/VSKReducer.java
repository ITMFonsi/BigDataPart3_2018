package UE4;

import UE3.TemperaturePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VSKReducer extends Reducer<TemperaturePair, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(TemperaturePair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

    }
}
