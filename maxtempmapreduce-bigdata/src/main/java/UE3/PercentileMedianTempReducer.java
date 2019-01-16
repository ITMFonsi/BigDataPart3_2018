package UE3;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PercentileMedianTempReducer extends Reducer<TemperaturePair, DoubleWritable, Text, DoubleWritable>  {

    ArrayList<Double> temperatureList = new ArrayList<Double>();
    double percentileValue = 0.0;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String value1 = conf.get("perc");
        percentileValue = Double.valueOf(value1);
    }

    @Override
    protected void reduce(TemperaturePair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            temperatureList.add(value.get());
        }

        Percentile percentile = new Percentile();
        double[] array = ArrayUtils.toPrimitive(temperatureList.toArray(new Double[0]));
        percentile.setData(array);
        percentile.setQuantile(percentileValue);
        double result = percentile.evaluate();
        result = result / 10;

        context.write(new Text(key.getYearMonth()), new DoubleWritable(result));
    }


}
