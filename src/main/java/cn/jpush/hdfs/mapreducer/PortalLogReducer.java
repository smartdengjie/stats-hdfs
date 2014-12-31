/**
 * 
 */
package cn.jpush.hdfs.mapreducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author dengjie
 * @date 2014年12月29日
 * @description TODO
 */
public class PortalLogReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	for (Text v : values) {
	    context.write(v, NullWritable.get());
	}
    }

}
