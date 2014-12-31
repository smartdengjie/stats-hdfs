package cn.jpush.hdfs.utils;

public class ConfigUtils {

    public interface HOSTNAME {
	final String MASTER = "master";
	final String IP = "10.211.55.17";
	final int PORT = 9000;
    }

    public interface MEMORY {
	final int MAXSIZE = 4096;
    }

    public interface HDFS {
	String INPUT_PATH = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/ncdc/2011/999999-96404-2011.gz";
	String OUTPUT_PATH = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/output/ncdc/2011/%s";
	String WORDCOUNT_IN = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/test/%s";
	String WORDCOUNT_OUT = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/output/result/%s";
	String LOGDFS_PATH = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/resource/access_2013_05_30.log";
	String LOGDFS_RESULT = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/logdfs/%s";
	String PORTAL_PATH = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/resource/logs/localhost_access_log.%s.txt";
	String PORTAL_RESULT = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hdfs/logdfs/portal/%s";
    }

    public interface HIVE {
	String CREATE_LOCAL_TBL = "hive.table.create.local";
	String CREATE_HDFS_TBL = "hive.table.create.hdfs";
	String DROP_TBL = "hive.table.drop";
	String LOAD_LOCAL = "hive.table.load.local";
	String LOAD_HDFS = "hive.table.load.hdfs";
	String QUERY = "hive.table.query";
	String HDFS_FILE_PATH = "hdfs://" + HOSTNAME.MASTER + ":" + HOSTNAME.PORT + "/home/hive/warehouse/%s";
    }

}
