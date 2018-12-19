import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeatherDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        JobConf config = new JobConf(getConf(), WeatherDriver.class);
        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(DoubleWritable.class);
        config.setMapperClass(WeatherMapper.class);
        config.setReducerClass(WeatherReducer.class);

        FileInputFormat.addInputPath(config, new Path(strings[0]));
        FileOutputFormat.setOutputPath(config, new Path(strings[1]));

        Job job = Job.getInstance(config, "Max Temp");

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WeatherDriver(), args);
        System.exit(exitCode);
    }
}
