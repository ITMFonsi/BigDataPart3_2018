import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int EXCLUDED_TEMP = 9999;
    private static final String QUALITY_CODE = "[01459]";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dataLine = value.toString();
        String dataYear = dataLine.substring(15, 19);
        double temp ;
        if(dataLine.charAt(87) == '+') {
            temp = Double.parseDouble(dataLine.substring(88, 92));
        } else {
            temp = Double.parseDouble(dataLine.substring(87, 92));
        }
        String qual = dataLine.substring(92,93);
        if(temp != EXCLUDED_TEMP && qual.matches(QUALITY_CODE)) {
            context.write(new Text(dataYear), new DoubleWritable(temp));

        }
    }
}
