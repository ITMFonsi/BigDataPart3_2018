import UE3.MedianTempReducer;
import UE3.SecSortTempMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MedianMapReduceTest {


    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new SecSortTempMapper();
        Reducer reducer = new MedianTempReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);


    }

    @Test
    public void testMean1901() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src/test/resources/1901");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-14.0));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMean1902() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src/test/resources/1902.txt");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1902"), new DoubleWritable(-6.0));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduce1903() throws IOException {
        List<String> lines = getInputFromFile("src/test/resources/1903.all");

        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1903"), new DoubleWritable(-44.0));
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
