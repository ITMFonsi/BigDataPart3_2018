import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WeatherMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int EXCLUDED_TEMP = 9999;
    private static final String QUALITY_CODE = "[01459]";

    public void map(LongWritable longWritable, Text text, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        String dataLine = text.toString();
        String dataYear = dataLine.substring(15, 19);
        double temp ;
        if(dataLine.charAt(87) == '+') {
            temp = Double.parseDouble(dataLine.substring(88, 92));
        } else {
            temp = Double.parseDouble(dataLine.substring(87, 92));
        }
        String qual = dataLine.substring(92,93);
        if(temp != EXCLUDED_TEMP && qual.matches(QUALITY_CODE)) {
            outputCollector.collect(new Text(dataYear), new DoubleWritable(temp));

        }
    }
}
