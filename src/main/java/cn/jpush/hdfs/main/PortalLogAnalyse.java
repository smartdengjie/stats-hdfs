/**
 * 
 */
package cn.jpush.hdfs.main;

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

import cn.jpush.hdfs.mapreducer.PortalLogMapper;
import cn.jpush.hdfs.mapreducer.PortalLogReducer;
import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * @author dengjie
 * @date 2014年12月29日
 * @description TODO
 */
public class PortalLogAnalyse extends Configured implements Tool {

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
	final Job job = new Job(new Configuration(), LogCleanMR.class.getSimpleName());
	job.setJarByClass(PortalLogMapper.class);
	job.setMapperClass(PortalLogMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(PortalLogReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	FileInputFormat.setInputPaths(job, args[0]);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	int status = job.waitForCompletion(true) ? 0 : 1;
	return status;
    }

    public static void main(String[] args) throws Exception {
	int res = 0;
	for (int i = 0; i < 1; i++) {
	    String in = "2014-12-2" + i;
	    String out = "2014_12_2" + i;
	    args = new String[] { String.format(ConfigUtils.HDFS.PORTAL_PATH, in), String.format(ConfigUtils.HDFS.PORTAL_RESULT, out) };
	    res = ToolRunner.run(new Configuration(), new PortalLogAnalyse(), args);
	}
	System.exit(res);
    }

}
