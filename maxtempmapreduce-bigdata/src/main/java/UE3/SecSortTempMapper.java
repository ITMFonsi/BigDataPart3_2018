package UE3;


import UE2.NcdcRecordParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortTempMapper extends Mapper<LongWritable, Text, TemperaturePair, DoubleWritable> {
    private TemperaturePair temperaturePair = new TemperaturePair();
    private static final int MISSING = 9999;

    public NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            temperaturePair.setYear(parser.getYear());
            temperaturePair.setTemperature(parser.getAirTemperature());
            context.write(temperaturePair, new DoubleWritable(temperaturePair.getDoubleTemp()));
        }
    }

}
