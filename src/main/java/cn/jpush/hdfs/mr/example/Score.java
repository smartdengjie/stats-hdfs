/**
 * 
 */
package cn.jpush.hdfs.mr.example;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
 * @author dengjie
 * @date 2014年12月2日
 * @description 统计chinese，English和math三门功课的平均成绩
 */
public class Score {

    private static Logger log = LoggerFactory.getLogger(Score.class);

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    // 将输入的纯文本的数据转成String
	    String line = value.toString();

	    // 将数据按行进行分割
	    StringTokenizer token = new StringTokenizer(line, "\n");
	    while (token.hasMoreElements()) {
		// 每行按空格分割
		StringTokenizer tokenLine = new StringTokenizer(token.nextToken());
		String strName = tokenLine.nextToken();// 姓名
		String strScore = tokenLine.nextToken();// 成绩

		Text name = new Text(strName);
		int scoreInt = Integer.parseInt(strScore);
		context.write(name, new IntWritable(scoreInt));

	    }
	}

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

	    int sum = 0;
	    int count = 0;

	    for (IntWritable intWritable : values) {
		sum += intWritable.get();// 计算总分
		count++;// 统计总科目
	    }
	    int avg = sum / count;
	    context.write(key, new IntWritable(avg));
	}

    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();
	long random = new Random().nextLong();
	log.info("random -> " + random);
	args = new String[] { String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "math"), String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "english"), String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "chinese"), String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random) };

	Job job = new Job(conf, "Score");
	job.setJarByClass(Score.class);
	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	for (int i = 0; i < args.length - 1; i++) {
	    FileInputFormat.addInputPath(job, new Path(args[i]));
	}
	FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
