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
import java.io.IOException;
import java.util.List;

public class MeanMapReduceTest {

    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new MeanTempMapper();
        Reducer reducer = new MeanTempReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMean1901() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src\\test\\resources\\1901");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(4.6698507007922));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMean1902() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src\\test\\resources\\1902.txt");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1902"), new DoubleWritable(2.165955826351866));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMean1903() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src\\test\\resources\\1903.all");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1903"), new DoubleWritable(4.824174473967132));
        mapReduceDriver.runTest();
    }

}
