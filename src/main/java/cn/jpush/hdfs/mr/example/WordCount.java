package cn.jpush.hdfs.mr.example;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * 
 * @author dengjie
 * @date 2014年11月29日
 * @description TODO
 */
public class WordCount {

    private static Logger log = LoggerFactory.getLogger(WordCount.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens()) {
		word.set(itr.nextToken());
		context.write(word, one);
	    }
	}
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable val : values) {
		sum += val.get();
	    }
	    result.set(sum);
	    context.write(key, result);
	}
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	long random = new Random().nextLong();
	log.info("random -> " + random);
	Job job = new Job(conf, "word count");
	job.setJarByClass(WordCount.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "test.txt")));
	FileInputFormat.addInputPath(job, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "word.txt")));
	FileOutputFormat.setOutputPath(job, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random)));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
