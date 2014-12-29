/**
 * 
 */
package cn.jpush.hdfs.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.jpush.hdfs.mapreducer.LogMapper;
import cn.jpush.hdfs.mapreducer.LogReducer;
import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * @author dengjie
 * @date 2014年12月24日
 * @description 将清洗后的日志重新存放指定的hdfs上
 */
public class LogCleanMR extends Configured implements Tool {

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
	final Job job = new Job(new Configuration(), LogCleanMR.class.getSimpleName());
	job.setJarByClass(LogCleanMR.class);
	job.setMapperClass(LogMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(LogReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	FileInputFormat.setInputPaths(job, args[0]);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	int status = job.waitForCompletion(true) ? 0 : 1;
	return status;
    }

    public static void main(String[] args) throws Exception {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
	args = new String[] { ConfigUtils.HDFS.LOGDFS_PATH, String.format(ConfigUtils.HDFS.LOGDFS_RESULT, sdf.format(new Date())) };
	int res = ToolRunner.run(new Configuration(), new LogCleanMR(), args);
	System.exit(res);
    }

}
