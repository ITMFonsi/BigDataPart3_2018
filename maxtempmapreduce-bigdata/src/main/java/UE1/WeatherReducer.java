package UE1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WeatherReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxTemperature = -999.9;
        Iterator iterator = values.iterator();
        while(iterator.hasNext()) {
            double curr = ((DoubleWritable)iterator.next()).get();
            if(maxTemperature < curr) {
                maxTemperature = curr;
            }
        }
        //the temperature in the data-set is represented in celsius times 10
        maxTemperature = maxTemperature /10;
        context.write(text, new DoubleWritable(maxTemperature));
    }
}
