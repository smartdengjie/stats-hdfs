package cn.jpush.hdfs.mr.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * This is the trivial map/reduce program that does absolutely nothing other
 * than use the framework to fragment and sort the input values.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar sort [-r <i>reduces</i>]
 * [-inFormat <i>input format class</i>] [-outFormat <i>output format class</i>]
 * [-outKey <i>output key class</i>] [-outValue <i>output value class</i>]
 * [-totalOrder <i>pcnt</i> <i>num samples</i> <i>max splits</i>] <i>in-dir</i>
 * <i>out-dir</i>
 */
@SuppressWarnings("deprecation")
public class Sort {
    private static Logger log = LoggerFactory.getLogger(Sort.class);

    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    context.write(new IntWritable(Integer.parseInt(value.toString())), one);
	}
    }

    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private static IntWritable count = new IntWritable(0);

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    for (IntWritable val : values) {
		context.write(key, count);
		count.set(count.get() + 1);
	    }
	}
    }

    public static void main(String[] args) throws Exception {
	long random = new Random().nextLong();
	log.info("random -> " + random);
	args = new String[] { String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "sort.txt"), String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random) };

	Configuration conf = new Configuration();

	Job job = new Job(conf, "Sort");
	job.setJarByClass(Sort.class);

	job.setMapperClass(SortMapper.class);
	job.setReducerClass(SortReducer.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
