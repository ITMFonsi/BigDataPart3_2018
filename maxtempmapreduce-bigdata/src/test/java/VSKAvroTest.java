import UE4.VSKMapper;
import UE4.VSKReducer;
import UE4.WSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class VSKAvroTest {

    private Mapper mapper;
    private MapDriver<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>> mapDriver;

    private Reducer reducer;
    private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    private MapDriver<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>> configureMapDriver(Mapper mapper) throws Exception{
        MapDriver<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>> mapDriver = MapDriver.newMapDriver(mapper);

        Configuration configuration = mapDriver.getConfiguration();

        // Add AvroSerialization to the configuration
        // (copy over the default serializations for deserializing the mapper inputs)
        String[] serializations = configuration.getStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY);
        String[] newSerializations = Arrays.copyOf(serializations, serializations.length + 1);
        newSerializations[serializations.length] = AvroSerialization.class.getName();
        configuration.setStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY, newSerializations);

        //Configure AvroSerialization by specifying the key writer and value writer schemas
        AvroSerialization.setKeyWriterSchema(configuration, Schema.create(Schema.Type.STRING));
        AvroSerialization.setValueWriterSchema(configuration, WSchema.INSTANCE.weatherRecordSchema());


        return mapDriver;
    }

    private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> configureMapReduceDriver(Mapper mapper, Reducer reducer) {

        MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        Configuration configuration = mapReduceDriver.getConfiguration();

        // Add AvroSerialization to the configuration
        // (copy over the default serializations for deserializing the mapper inputs)
        String[] serializations = configuration.getStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY);
        String[] newSerializations = Arrays.copyOf(serializations, serializations.length + 1);
        newSerializations[serializations.length] = AvroSerialization.class.getName();
        configuration.setStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY, newSerializations);

        //Configure AvroSerialization by specifying the key writer and value writer schemas
        AvroSerialization.setKeyWriterSchema(configuration, Schema.create(Schema.Type.STRING));
        AvroSerialization.setValueWriterSchema(configuration, WSchema.INSTANCE.weatherRecordSchema());

        return mapReduceDriver;
    }

    @Before
    public void setUp() throws Exception {
        mapper = new VSKMapper();

        mapDriver = configureMapDriver(mapper);

        reducer = new VSKReducer();

        mapReduceDriver = configureMapReduceDriver(mapper, reducer);
    }



    @Test
    public void testMapReduce1901() throws IOException {
        List<String> lines = getInputFromFile("src/test/resources/1901");

        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.runTest();
    }

    protected static List<String> getInputFromFile(String path) {
        List<String> lines = new LinkedList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }



}
