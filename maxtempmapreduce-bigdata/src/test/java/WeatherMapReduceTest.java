import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;


public class WeatherMapReduceTest {


    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new WeatherMapper();
        Reducer reducer = new WeatherReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("0035029070999991902010113004+64333+023450FM-12+000599999V0201401N011819999999N0000001N9-01001+99999100311ADDGF104991999999999999999999MW1381"));
        mapDriver.withOutput(new Text("1902"), new DoubleWritable(-100.0));

        mapDriver.withInput(new LongWritable(), new Text("0029029070999991902010720004+64333+023450FM-12+000599999V0202501N027819999999N0000001N9-00111+99999098351ADDGF102991999999999999999999"));
        mapDriver.withOutput(new Text("1902"), new DoubleWritable(-11.0));

        mapDriver.withInput(new LongWritable(), new Text("0035029070999991902032413004+64333+023450FM-12+000599999V0200501N013919999999N0000001N9-00611+99999101271ADDGF108991999999999999999999MW1381"));
        mapDriver.withOutput(new Text("1902"), new DoubleWritable(-61.0));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(240.0));
        values.add(new DoubleWritable(323.0));
        values.add(new DoubleWritable(210.0));
        values.add(new DoubleWritable(170.0));
        values.add(new DoubleWritable(240.0));
        values.add(new DoubleWritable(20.0));
        values.add(new DoubleWritable(55.0));
        values.add(new DoubleWritable(369.0));
        reduceDriver.withInput(new Text("1902"), values);
        reduceDriver.withOutput(new Text("1902"), new DoubleWritable(36.9));
        reduceDriver.runTest();
    }

    @Test
    public void testReducerMix() throws IOException {
        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(-40.0));
        values.add(new DoubleWritable(323.0));
        values.add(new DoubleWritable(-10.0));
        values.add(new DoubleWritable(170.0));
        values.add(new DoubleWritable(-240.0));
        values.add(new DoubleWritable(20.0));
        values.add(new DoubleWritable(-55.0));
        values.add(new DoubleWritable(-369.0));
        reduceDriver.withInput(new Text("1902"), values);
        reduceDriver.withOutput(new Text("1902"), new DoubleWritable(32.3));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce1902() throws IOException {
        List<String> lines = getInputFromFile("src\\test\\resources\\1902.txt");

        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1902"), new DoubleWritable(24.4));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduce1901() throws IOException {
        List<String> lines = getInputFromFile("src\\test\\resources\\1901");

        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1901"), new DoubleWritable(31.7));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduce1903() throws IOException {
        List<String> lines = getInputFromFile("src\\test\\resources\\1903.all");

        for(String line : lines) {
            mapReduceDriver.withInput(new LongWritable(), new Text(line));
        }
        mapReduceDriver.withOutput(new Text("1903"), new DoubleWritable(28.9));
        mapReduceDriver.runTest();
    }

    private List<String> getInputFromFile(String path) {
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


