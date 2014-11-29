package cn.jpush.hdfs.main;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.mapreducer.MaxTemperatureMapper;
import cn.jpush.hdfs.mapreducer.MaxTemperatureReducer;
import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * 
 * @author dengjie
 * @date 2014年11月22日
 * @description TODO
 */
public class MaxTemprature {

    private static Logger log = LoggerFactory.getLogger(MaxTemprature.class);

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
	try {
	    long start = System.currentTimeMillis();
	    Configuration conf = new Configuration();
	    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization");
	    // conf.set("io.sort.mb", "512");
	    // conf.setBoolean("mapreduce.map.output.compress", true);
	    // conf.setClass("mapreduce.map.output.compress.codec",
	    // GzipCodec.class,CompressionCodec.class);
	    long rondom = new Random().nextLong();
	    Job job = new Job(conf, "Max temperature");
	    job.setJarByClass(MaxTemprature.class);
	    job.setMapperClass(MaxTemperatureMapper.class);
	    job.setCombinerClass(MaxTemperatureReducer.class);
	    job.setReducerClass(MaxTemperatureReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(ConfigUtils.HDFS.INPUT_PATH));
	    FileOutputFormat.setOutputPath(job, new Path(String.format(ConfigUtils.HDFS.OUTPUT_PATH, rondom)));

	    log.info("rondom -> " + rondom);
	    log.info("times -> " + (System.currentTimeMillis() - start));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error(e.getMessage());
	}
    }

}
