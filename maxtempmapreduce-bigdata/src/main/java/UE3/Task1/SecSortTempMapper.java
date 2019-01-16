package UE3.Task1;


import UE2.NcdcRecordParser;
import UE3.TemperaturePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortTempMapper extends Mapper<LongWritable, Text, TemperaturePair, DoubleWritable> {
    private TemperaturePair temperaturePair = new TemperaturePair();

    public NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            temperaturePair.setYear(parser.getYear());
            temperaturePair.setTemperature(Integer.MIN_VALUE);
            context.write(temperaturePair, new DoubleWritable(parser.getAirTemperature()));
        }
    }

}
