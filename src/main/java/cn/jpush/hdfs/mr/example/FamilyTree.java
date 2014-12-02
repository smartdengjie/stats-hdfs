/**
 * 
 */
package cn.jpush.hdfs.mr.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.mr.example.Sort.SortMapper;
import cn.jpush.hdfs.mr.example.Sort.SortReducer;
import cn.jpush.hdfs.utils.ConfigUtils;

/**
 * @author dengjie
 * @date 2014年12月2日
 * @description TODO
 */
public class FamilyTree {

    private static Logger log = LoggerFactory.getLogger(FamilyTree.class);
    private static int MAX_FAMILY_MEMBER = 20;// 预计祖辈有5个小孩，父辈有4个小孩，这是预估，暂时这样处理
    private static int time = 0;

    /*
     * map输出分割child和parent，正序右表，反序坐标（注意添加左右表的标示符）
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	    String childName = new String();// 孩子名
	    String parentName = new String();// 父母名
	    String relationType = new String();// 左右表标示符

	    StringTokenizer token = new StringTokenizer(value.toString());// 获取一行文本数据
	    String[] values = new String[2];
	    int i = 0;
	    while (token.hasMoreElements()) {
		values[i] = token.nextToken();
		i++;
	    }

	    if (values[0].compareTo("child") != 0) {
		childName = values[0];
		parentName = values[1];
		relationType = "l";
		context.write(new Text(values[1]), new Text(relationType + "-" + childName + "-" + parentName));

		relationType = "r";
		context.write(new Text(values[0]), new Text(relationType + "-" + childName + "-" + parentName));
	    }

	}

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

	    // 输出表头
	    if (0 == time) {
		context.write(new Text("grandchild"), new Text("grandparent"));
		time++;
	    }

	    int grandChildNum = 0;
	    String[] grandChild = new String[MAX_FAMILY_MEMBER];
	    int grandParentNum = 0;
	    String[] grandParent = new String[MAX_FAMILY_MEMBER];

	    Iterator<Text> ite = values.iterator();
	    while (ite.hasNext()) {
		String record = ite.next().toString();
		int len = record.length();
		int i = 2;
		if (0 == len) {
		    continue;
		}

		// 获取表的标示符
		char relationType = record.charAt(0);
		String childName = new String();
		String parentName = new String();

		while (record.charAt(i) != '-') {
		    childName += record.charAt(i);
		    i++;
		}
		i += 1;

		while (i < len) {
		    parentName += record.charAt(i);
		    i++;
		}
		if ('l' == relationType) {
		    grandChild[grandChildNum] = childName;
		    grandChildNum++;
		}

		if ('r' == relationType) {
		    grandParent[grandParentNum] = parentName;
		    grandParentNum++;
		}
	    }

	    if (0 != grandChildNum && 0 != grandParentNum) {
		for (int m = 0; m < grandChildNum; m++) {
		    for (int n = 0; n < grandParentNum; n++) {
			// 输出结果
			context.write(new Text(grandChild[m]), new Text(grandParent[n]));
		    }
		}
	    }
	}

    }
    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	long random = new Random().nextLong();// 生产随机数
	log.info("random -> " + random);
	args = new String[] { String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "family"), String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random) };

	Configuration conf = new Configuration();

	Job job = new Job(conf, "Sort");
	job.setJarByClass(FamilyTree.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
