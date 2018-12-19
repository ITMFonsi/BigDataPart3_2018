import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WeatherReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        double maxTemperature= 0;

        while(iterator.hasNext()) {
            double curr = iterator.next().get();
            if(maxTemperature < curr) {
                maxTemperature = curr;
            }
        }
        //the temperature in the data-set is represented in celsius times 10
        maxTemperature = maxTemperature /10;
        outputCollector.collect(text, new DoubleWritable(maxTemperature));
    }
}
