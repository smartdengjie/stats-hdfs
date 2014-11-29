package cn.jpush.hdfs.mr.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.utils.ConfigUtils;

import com.google.common.base.Charsets;

/**
 * 统计出key所需要的次数和key的总长度
 * @author dengjie
 * @date 2014年11月29日
 * @description TODO
 */
public class WordMean extends Configured implements Tool {

    private double mean = 0;

    private static Logger log = LoggerFactory.getLogger(WordMean.class);
    private final static Text COUNT = new Text("count");
    private final static Text LENGTH = new Text("length");
    private final static LongWritable ONE = new LongWritable(1);

    /**
     * Maps words from line of text into 2 key-value pairs; one key-value pair
     * for counting the word, another for counting its length.
     */
    public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {

	private LongWritable wordLen = new LongWritable();

	/**
	 * Emits 2 key-value pairs for counting the word and its length. Outputs
	 * are (Text, LongWritable).
	 * 
	 * @param value
	 *            This will be a line of text coming in from our input file.
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens()) {
		String string = itr.nextToken();
		this.wordLen.set(string.length());
		context.write(LENGTH, this.wordLen);
		context.write(COUNT, ONE);
	    }
	}
    }

    /**
     * Performs integer summation of all the values for each key.
     */
    public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable sum = new LongWritable();

	/**
	 * Sums all the individual values within the iterator and writes them to
	 * the same key.
	 * 
	 * @param key
	 *            This will be one of 2 constants: LENGTH_STR or COUNT_STR.
	 * @param values
	 *            This will be an iterator of all the values associated with
	 *            that key.
	 */
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

	    int theSum = 0;
	    for (LongWritable val : values) {
		theSum += val.get();
	    }
	    sum.set(theSum);
	    context.write(key, sum);
	}
    }

    /**
     * Reads the output file and parses the summation of lengths, and the word
     * count, to perform a quick calculation of the mean.
     * 
     * @param path
     *            The path to find the output file in. Set in main to the output
     *            directory.
     * @throws IOException
     *             If it cannot access the output directory, we throw an
     *             exception.
     */
    private double readAndCalcMean(Path path, Configuration conf) throws IOException {
//	FileSystem fs = FileSystem.get(conf);// 分布式
	FileSystem fs = new Path(path, "part-r-00000").getFileSystem(conf);// 伪分布式
	Path file = new Path(path, "part-r-00000");

	if (!fs.exists(file)) {
	    throw new IOException("Output not found!");
	}

	BufferedReader br = null;

	// average = total sum / number of elements;
	try {
	    br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

	    long count = 0;
	    long length = 0;

	    String line;
	    while ((line = br.readLine()) != null) {
		StringTokenizer st = new StringTokenizer(line);

		// grab type
		String type = st.nextToken();

		// differentiate
		if (type.equals(COUNT.toString())) {
		    String countLit = st.nextToken();
		    count = Long.parseLong(countLit);
		} else if (type.equals(LENGTH.toString())) {
		    String lengthLit = st.nextToken();
		    length = Long.parseLong(lengthLit);
		}
	    }

	    double theMean = (((double) length) / ((double) count));
	    System.out.println("The mean is: " + theMean);
	    return theMean;
	} finally {
	    if (br != null) {
		br.close();
	    }
	}
    }

    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new WordMean(), args);
    }

    public int run(String[] args) throws Exception {
	long random = new Random().nextLong();
	log.info("random -> " + random);

	Configuration conf = getConf();

	@SuppressWarnings("deprecation")
	Job job = new Job(conf, "word mean");
	job.setJarByClass(WordMean.class);
	job.setMapperClass(WordMeanMapper.class);
	job.setCombinerClass(WordMeanReducer.class);
	job.setReducerClass(WordMeanReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	FileInputFormat.addInputPath(job, new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_IN, "word.txt")));
	Path outputpath = new Path(String.format(ConfigUtils.HDFS.WORDCOUNT_OUT, random));
	FileOutputFormat.setOutputPath(job, outputpath);
	boolean result = job.waitForCompletion(true);
	mean = readAndCalcMean(outputpath, conf);

	return (result ? 0 : 1);
    }

    /**
     * Only valuable after run() called.
     * 
     * @return Returns the mean value.
     */
    public double getMean() {
	return mean;
    }
}