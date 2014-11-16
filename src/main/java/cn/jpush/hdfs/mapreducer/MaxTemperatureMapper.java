package cn.jpush.hdfs.mapreducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	// 2011 06 21 01 80 44 10035 340 20 -9999 -9999 -9999
	// 2011010100-56 -98 10264 340 110 8 -9999 -1
	private static final int MISSING = 9999;
	private static Logger logger = LoggerFactory
			.getLogger(MaxTemperatureMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context) {
		try {
			if (value.getLength() <= 19) {
				logger.info("length is error");
				return;
			}
			String line = value.toString();
			String year = line.substring(0, 4);
			String month = line.substring(5, 7);
			try {
				int airTemperature = Integer.parseInt(line.substring(15, 19)
						.replace(" ", ""));
				if (airTemperature != MISSING) {
					context.write(new Text(year + month), new IntWritable(
							airTemperature));
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Integer parse error -> " + e.getMessage());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("line string substring error -> " + ex.getMessage());
		}
	}

}
