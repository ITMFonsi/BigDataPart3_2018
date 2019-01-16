import UE3.Task2.SortByTempMapper;
import UE3.Task2.SortByTempReducer;
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

public class SortByTempTest {

    MapDriver<LongWritable, Text, DoubleWritable, Text> mapDriver;
    ReduceDriver<DoubleWritable, Text, DoubleWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new SortByTempMapper();
        Reducer reducer = new SortByTempReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);


    }

  /*  @Test
    public void testSortByTemp1901() throws IOException {
        List<String> lines = WeatherMapReduceTest.getInputFromFile("src/test/resources/allTemps.txt");
        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        //mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(-14.0));
        mapReduceDriver.runTest();
    }
*/



}
