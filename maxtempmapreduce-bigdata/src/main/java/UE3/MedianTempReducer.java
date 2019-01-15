package UE3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MedianTempReducer extends Reducer<TemperaturePair, DoubleWritable, Text, DoubleWritable> {

    ArrayList<Double> temperatureList = new ArrayList<Double>();

    @Override
    protected void reduce(TemperaturePair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double median = 0;
        for (DoubleWritable value : values) {
            temperatureList.add(value.get());
        }
        int size  = temperatureList.size();
        if(size%2 == 0){
            int half = size/2;
            median  = temperatureList.get(half);
        } else {
            int half = (size + 1)/2;
            median = temperatureList.get(half -1);
        }

        context.write(key.getYearMonth(), new DoubleWritable(median));
    }
}
