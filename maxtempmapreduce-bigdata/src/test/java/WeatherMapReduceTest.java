import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WeatherMapReduceTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WeatherMapper mapper = new WeatherMapper();
        WeatherReducer reducer = new WeatherReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(""));
        mapDriver.withOutput(new Text(""), new IntWritable());
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("key"), values);
        reduceDriver.withOutput(new Text("key"), new IntWritable());
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("testinputstring"));
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(123));
        values.add(new IntWritable(123));
        mapReduceDriver.withOutput(new Text("year"), new IntWritable(123));
        mapReduceDriver.runTest();
    }
}


