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
 * @description Map任务，对日志文件进行处理得到数据源，然后对数据源进行分割和重组
 */
public class IPMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger log = LoggerFactory.getLogger(IPMapper.class);

    // NullWritable充当占位符，无意义
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	try {
	    // 处理数据
	    String[] lineSplit = line.split(" ");
	    if (lineSplit.length < 6) {
		log.error("Line Data Length is Error, Real Length is -> " + lineSplit.length);
		return;
	    }
	    String date = lineSplit[0];
	    String time = lineSplit[1];
	    String ip = lineSplit[6];
	    // 日期+时间作为key，ip作为value
	    context.write(new Text(date + " " + time), new Text(ip));
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("Map Error -> " + ex.getMessage());
	    context.getCounter(HDFSCountErrorUtils.COUNTER.LINESKIP).increment(1);// 如果出错，计数器自增
	}
    }

}
