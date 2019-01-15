package UE3;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortTempMapper extends Mapper<LongWritable, Text, TemperaturePair, DoubleWritable> {
    private TemperaturePair temperaturePair = new TemperaturePair();
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String yearMonth = line.substring(15, 21);

        int tempStartPosition = 87;

        if (line.charAt(tempStartPosition) == '+') {
            tempStartPosition += 1;
        }

        int temp = Integer.parseInt(line.substring(tempStartPosition, 92));

        if (temp != MISSING) {
            temperaturePair.setYearMonth(yearMonth);
            temperaturePair.setTemperature(temp);
            context.write(temperaturePair, new DoubleWritable(temperaturePair.getDoubleTemp()));
        }
    }

}

