package UE3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PercentileMedianTempDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("perc", strings[2]);

        Job job = Job.getInstance(conf, "Percentile Median Temp");
        job.setMapOutputKeyClass(TemperaturePair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);

        job.setMapperClass(SecSortTempMapper.class);
        job.setReducerClass(MedianTempReducer.class);
        job.setPartitionerClass(MedianTempPartitioner.class);
        job.setSortComparatorClass(MedianKeyComparator.class);


        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MedianTempDriver(), args);
        System.exit(exitCode);
    }
}
