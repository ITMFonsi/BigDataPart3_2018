package UE4;


import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.File;
import java.io.IOException;

public class VSKReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {


    @Override
    protected void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        GenericRecord record = null;
        for(AvroValue<GenericRecord> value : values) {
            record = new GenericData.Record(WSchema.INSTANCE.weatherRecordSchema());
            newVSKRecord(record);
        }
    }

    private void newVSKRecord(GenericRecord value) {
        VSKSchema schema = new VSKSchema();

        schema.setYear((String) value.get("year"));
        schema.setVariance((Double) value.get("variance"));
        schema.setSkewness((Double) value.get("skewness"));
        schema.setKurtosis((Double) value.get("kurtosis"));

        serializeRecord(schema);
    }

    private void serializeRecord(VSKSchema schema) {
        File avroOutput = new File("vsk-test.avro");
        try {
            DatumWriter<VSKSchema> bdPersonDatumWriter = new SpecificDatumWriter<VSKSchema>(VSKSchema.class);
            DataFileWriter<VSKSchema> dataFileWriter = new DataFileWriter<VSKSchema>(bdPersonDatumWriter);
            dataFileWriter.create(schema.getSchema(), avroOutput);
            dataFileWriter.append(schema);
            dataFileWriter.close();
        } catch (IOException e) {System.out.println("Error writing Avro");}

    }
}
