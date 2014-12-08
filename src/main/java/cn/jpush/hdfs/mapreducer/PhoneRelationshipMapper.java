/**
 * 
 */
package cn.jpush.hdfs.mapreducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.utils.HDFSCountErrorUtils;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description map任务，主叫作为value，被叫作为key
 */
public class PhoneRelationshipMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger log = LoggerFactory.getLogger(PhoneRelationshipMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	try {
	    // 数据处理
	    String[] lineSpilt = line.split(" ");
	    String caller = lineSpilt[0];// 主叫
	    String called = lineSpilt[1];// 被叫
	    context.write(new Text(called), new Text(caller));
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("Phone Mapper Error -> " + ex.getMessage());
	    context.getCounter(HDFSCountErrorUtils.COUNTER.LINESKIP).increment(1);
	}
    }

}
