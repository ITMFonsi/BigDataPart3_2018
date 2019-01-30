package UE4;

import UE2.NcdcRecordParser;
import at.fhv.VSKSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class VSKMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>>{

    public NcdcRecordParser parser = new NcdcRecordParser();
    private GenericRecord record = new GenericData.Record(WSchema.INSTANCE.weatherRecordSchema());

    private HashMap<String, LinkedList<Double>> results;
    private ArrayList<Double> valuesTemp;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        results = new HashMap<>();
        valuesTemp = new ArrayList<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
           valuesTemp.add(parser.getAirTemperature());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        record.put("variance", VSKCalculator.calcVariance(valuesTemp));
        record.put("skewness", VSKCalculator.calcSkewness(valuesTemp));
        record.put("kurtosis", VSKCalculator.calcKurtosis(valuesTemp));
        context.write(new AvroKey(parser.getYear()), new AvroValue<>(record));
    }


}
