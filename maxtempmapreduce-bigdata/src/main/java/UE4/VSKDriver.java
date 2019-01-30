package UE4;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VSKDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "Variance, Skewness and Kurtosis with Avro Serialization");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, WSchema.INSTANCE.weatherRecordSchema());
        AvroJob.setOutputKeySchema(job, WSchema.INSTANCE.weatherRecordSchema());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(VSKMapper.class);
        job.setReducerClass(VSKReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new VSKDriver(), args);
        System.exit(exitCode);
    }
}
