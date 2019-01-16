package UE3.Task1;

import UE3.TemperaturePair;
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
            double sumOfMiddleElements = temperatureList.get(size/2) + temperatureList.get(size/2 -1);
             median  = sumOfMiddleElements / 2;
        } else {
             median = temperatureList.get(size / 2);
        }

        context.write(new Text(key.getYearMonth()), new DoubleWritable(median));
    }
}
