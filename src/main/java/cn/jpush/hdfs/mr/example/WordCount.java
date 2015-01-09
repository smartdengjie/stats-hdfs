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
 * @description Wordcount的例子是一个比较经典的mapreduce例子，可以叫做Hadoop版的hello world。
 *              它将文件中的单词分割取出，然后shuffle，sort（map过程），接着进入到汇总统计
 *              （reduce过程），最后写道hdfs中。基本流程就是这样。
 */
public class WordCount {

    private static Logger log = LoggerFactory.getLogger(WordCount.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	/*
	 * 源文件：a b b
	 * 
	 * map之后：
	 * 
	 * a 1
	 * 
	 * b 1
	 * 
	 * b 1
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens()) {
		word.set(itr.nextToken());// 按空格分割单词
		context.write(word, one);// 每次统计出来的单词+1
	    }
	}
    }

    /*
     * reduce之前：
     * 
     * a 1
     * 
     * b 1
     * 
     * b 1
     * 
     * reduce之后:
     * 
     * a 1
     * 
     * b 2
     */
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
	Configuration conf1 = new Configuration();
	Configuration conf2 = new Configuration();
	long random1 = new Random().nextLong();
	long random2 = new Random().nextLong();
	log.info("random1 -> " + random1 + ",random2 -> " + random2);
	Job job1 = new Job(conf1, "word count1");
	job1.setJarByClass(WordCount.class);
	job1.setMapperClass(TokenizerMapper.class);
	job1.setCombinerClass(IntSumReducer.class);
	job1.setReducerClass(IntSumReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(IntWritable.class);

	Job job2 = new Job(conf2, "word count2");
	job2.setJarByClass(WordCount.class);
	job2.setMapperClass(TokenizerMapper.class);
	job2.setCombinerClass(IntSumReducer.class);
	job2.setReducerClass(IntSumReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(IntWritable.class);
	// FileInputFormat.addInputPath(job, new
	// Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "test.txt")));
	FileInputFormat.addInputPath(job1, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "word")));
	FileOutputFormat.setOutputPath(job1, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random1)));
	FileInputFormat.addInputPath(job2, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "word")));
	FileOutputFormat.setOutputPath(job2, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random2)));

	boolean flag1 = job1.waitForCompletion(true);
	boolean flag2 = job1.waitForCompletion(true);
	if (flag1 && flag2) {
	    System.exit(0);
	} else {
	    System.exit(1);
	}

    }
}
