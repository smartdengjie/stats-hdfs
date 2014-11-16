package cn.jpush.hdfs.utils;

public class ConfigUtils {

	public interface HOSTNAME {
		final String IP = "10.211.55.11";// ubuntu server ip
	}

	public interface MEMORY {
		final int MAXSIZE = 4096;
	}

	public interface HDFS{
		String INPUT_PATH="hdfs://10.211.55.11:9000/home/dengjie/ncdc/data/999999-96404-2011.gz";
		String OUTPUT_PATH="hdfs://10.211.55.11:9000/usr/2011/output/max/%s";
		
	}

}
