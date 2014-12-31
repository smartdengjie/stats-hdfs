/**
 * 
 */
package cn.jpush.hdfs.mapreducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.jpush.hdfs.utils.LogParserFactory;

/**
 * @author dengjie
 * @date 2014年12月29日
 * @description TODO
 */
public class PortalLogMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	final String[] parsed = LogParserFactory.parse(value.toString());

	// 过掉开头的特定格式字符串
	if (parsed[2].startsWith("GET /")) {
	    parsed[2] = parsed[2].substring("GET /".length());
	} else if (parsed[2].startsWith("POST /")) {
	    parsed[2] = parsed[2].substring("POST /".length());
	}

	// 过滤结尾的特定格式字符串
	if (parsed[2].endsWith(" HTTP/1.1")) {
	    parsed[2] = parsed[2].substring(0, parsed[2].length() - " HTTP/1.1".length());
	} else if (parsed[2].endsWith(" HTTP/1.0")) {
	    parsed[2] = parsed[2].substring(0, parsed[2].length() - " HTTP/1.0".length());
	}

	String str = "";
	for (int i = 0; i < parsed.length; i++) {
	    if (i == (parsed.length - 1)) {
		str += parsed[i];
	    } else {
		str += parsed[i] + ",";
	    }
	}

	context.write(key, new Text(str));

    }

}
