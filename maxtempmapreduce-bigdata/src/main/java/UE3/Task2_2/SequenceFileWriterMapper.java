package UE3.Task2_2;

import UE2.NcdcRecordParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SequenceFileWriterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new DoubleWritable(parser.getAirTemperature()));
        }
    }

}
