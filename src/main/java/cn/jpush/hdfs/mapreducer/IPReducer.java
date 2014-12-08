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
 * @description 输出map后的结果
 */
public class IPReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	for (Text val : values) {
	    context.write(key, val);
	}
    }
}
