package cn.jpush.hdfs.mapreducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private static Logger logger = LoggerFactory
			.getLogger(MaxTemperatureReducer.class);

	@Override
	public void reduce(Text key, Iterable<IntWritable> value, Context context) {
		try {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable intWritable : value) {
				try {
					maxValue = Math.max(maxValue, intWritable.get());
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error("intWritable get error -> " + ex.getMessage());
				}
			}
			System.out.println("key->" + key + ",maxValue->" + maxValue);
			context.write(key, new IntWritable(maxValue));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

}
