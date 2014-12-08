/**
 * 
 */
package cn.jpush.hdfs.mapreducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description 每个val值代表拨打了被叫的主叫号码
 */
public class PhoneRelationshipReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String valueStr = "";
	String out = "";
	for (Text val : values) {
	    valueStr = val.toString();
	    out += valueStr + "|";
	}
	context.write(key, new Text(out));
    }

}
