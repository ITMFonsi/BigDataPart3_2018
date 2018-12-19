import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WeatherReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxTemperature= 0;

        while(iterator.hasNext()) {
            int curr = iterator.next().get();
            if(maxTemperature < curr) {
                maxTemperature = curr;
            }
        }
        //the temperature in the data-set is represented in celsius times 10
        maxTemperature = maxTemperature /10;
        outputCollector.collect(text, new IntWritable(maxTemperature));
    }
}
