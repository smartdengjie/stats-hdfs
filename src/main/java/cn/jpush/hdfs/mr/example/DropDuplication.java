/**
 * 
 */
package cn.jpush.hdfs.mr.example;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @date 2014年12月1日
 * @description 去掉重复数据（去重） 如：将元数据的一行数据设置为key，value为空；map后交给reduce直接输出
 */
public class DropDuplication {

    private static Logger log = LoggerFactory.getLogger(DropDuplication.class);

    public static class Map extends Mapper<Object, Text, Text, Text> {

	private static Text line = new Text();// 每行数据

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	    log.info("Map | key -> " + key + ",value -> " + value);
	    line = value;
	    context.write(line, new Text(""));
	}

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	    log.info("Reduce | key -> " + key);
	    context.write(key, new Text(""));
	}

    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();

	long random = new Random().nextLong();
	log.info("random -> " + random);
	args = new String[] { String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "file1"), String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "file2"), String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random) };

	Job job = new Job(conf, "Data DropDuplication");
	job.setJarByClass(DropDuplication.class);
	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
