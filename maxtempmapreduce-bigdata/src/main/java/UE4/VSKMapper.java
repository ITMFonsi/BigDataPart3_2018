package UE4;

import UE2.NcdcRecordParser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class VSKMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>>{

    public NcdcRecordParser parser = new NcdcRecordParser();
    private GenericRecord record = new GenericData.Record(WSchema.INSTANCE.weatherRecordSchema());
    private ArrayList<Double> valuesTemp;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
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
        record.put("year", parser.getYear());
        record.put("variance", VSKCalculator.calcVariance(valuesTemp));
        record.put("skewness", VSKCalculator.calcSkewness(valuesTemp));
        record.put("kurtosis", VSKCalculator.calcKurtosis(valuesTemp));
        context.write(new AvroKey<String>(parser.getYear()), new AvroValue<GenericRecord>(record));
    }



}
