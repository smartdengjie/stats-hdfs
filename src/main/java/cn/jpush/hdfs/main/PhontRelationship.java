/**
 * 
 */
package cn.jpush.hdfs.main;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.mapreducer.PhoneRelationshipMapper;
import cn.jpush.hdfs.mapreducer.PhoneRelationshipReducer;
import cn.jpush.hdfs.utils.ConfigUtils;
import cn.jpush.hdfs.utils.HDFSCountErrorUtils;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description 统计相同被叫的主叫名单
 */
public class PhontRelationship extends Configured implements Tool{

    private Logger log = LoggerFactory.getLogger(PhontRelationship.class);
    
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new PhontRelationship(), args);
	System.exit(res);
    }

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
	
	long random = new Random().nextLong();
	log.info("random -> " + random);
	// 第三个参数为抓取的单词目标
	args = new String[] { String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "phone"), String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random) };
	Configuration conf = getConf();
	Job job = new Job(conf, "Phone Relationship");
	job.setJarByClass(PhontRelationship.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(PhoneRelationshipMapper.class);
	job.setReducerClass(PhoneRelationshipReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	boolean status = job.waitForCompletion(true);

	// 输出任务完成情况
	log.info("Name -> " + job.getJobName());
	log.info("Success Status -> " + status);
	log.info("Skip Lines -> " + job.getCounters().findCounter(HDFSCountErrorUtils.COUNTER.LINESKIP).getValue());

	return status ? 0 : 1;
    }

}
