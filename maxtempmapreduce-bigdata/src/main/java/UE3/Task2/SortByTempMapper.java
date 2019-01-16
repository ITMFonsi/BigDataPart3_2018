package UE3.Task2;

import UE2.NcdcRecordParser;
import UE3.TemperaturePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByTempMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private TemperaturePair temperaturePair = new TemperaturePair();

    public NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new DoubleWritable(parser.getAirTemperature()), new Text(parser.getYear()));
        }
    }
}
