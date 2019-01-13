package UE2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MeanTempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if(parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new DoubleWritable(parser.getAirTemperature()));
        } else if(parser.isMalformedTemperature()) {
            context.getCounter(Temperature.MALFORMED).increment(1);
        } else if(parser.isMissingTemperature()) {
            context.getCounter(Temperature.MISSING).increment(1);
        }
        //counter for quality code
        context.getCounter("quality", parser.getQuality()).increment(1);
    }
}
