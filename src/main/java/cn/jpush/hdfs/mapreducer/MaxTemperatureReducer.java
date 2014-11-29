package cn.jpush.hdfs.mapreducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static Logger log = LoggerFactory.getLogger(MaxTemperatureReducer.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> value, Context context) {
	try {
	    int maxValue = Integer.MIN_VALUE;
	    for (IntWritable intWritable : value) {
		try {
		    maxValue = Math.max(maxValue, intWritable.get());
		} catch (Exception ex) {
		    ex.printStackTrace();
		    log.error("intWritable get error -> " + ex.getMessage());
		}
	    }
	    log.info("key->" + key + ",maxValue->" + maxValue);
	    context.write(key, new IntWritable(maxValue));
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error(e.getMessage());
	}
    }

}
