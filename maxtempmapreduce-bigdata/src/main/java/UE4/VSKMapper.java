package UE4;

import UE2.NcdcRecordParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class VSKMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    public NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {

        }
    }

    public double calcVariance(ArrayList<Double> values) {
        double var = 0;
        double sum = 0.0;
        double sum2 = 0.0;

        for (int i = 0; i < values.size(); i++) {
            values.set(i, Math.pow(values.get(i), 2));
        }

        for (int k = 0; k < values.size(); k++) {
            sum = sum + values.get(k);
        }

        sum = sum * (1/values.size());

        for (int j = 0; j < values.size(); j++) {
            sum2 = sum2 + values.get(j);
        }

        sum2 = sum * (1/values.size());
        sum2 = Math.pow(sum2, 2);

        var = (sum - sum2);

        return var;
    }

    public double calcSkewness(ArrayList<Double> values) {
        Double[] list = values.toArray(new Double[values.size()]);
        double[] d = ArrayUtils.toPrimitive(list);

        Skewness s = new Skewness();

        return s.evaluate(d, 0, d.length);
    }

    public double calcKurtosis(ArrayList<Double> values) {
        Double[] list = values.toArray(new Double[values.size()]);
        double[] d = ArrayUtils.toPrimitive(list);

        Kurtosis k = new Kurtosis();

        return k.evaluate(d, 0, d.length);
    }
}
