package UE4;

import UE3.TemperaturePair;
import at.fhv.VSKSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VSKReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {


    @Override
    protected void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        GenericRecord record = null;
        for(AvroValue<GenericRecord> value : values) {
            record = newWeatherRecord(value.datum());
        }
    }

    private GenericRecord newWeatherRecord(GenericRecord value) {
        GenericRecord record = new GenericData.Record(WSchema.INSTANCE.weatherRecordSchema());
        record.put("variance", value.get("variance"));
        record.put( "skewness", value.get("skewness"));
        record.put("kurtosis", value.get("kurtosis"));
        return record;
    }
}
