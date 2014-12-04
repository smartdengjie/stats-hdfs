package cn.jpush.hdfs.utils;

import java.sql.Connection;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBHelper {

    private static Logger logger = LoggerFactory.getLogger(DBHelper.class);

    private static final String DRIVER_HIVE = "org.apache.hadoop.hive.jdbc.HiveDriver";

    private DBHelper() {
    }

    public static Connection getHiveConn(String envFlag) {
	Connection connToHive = null;
	try {
	    System.setProperty("hadoop.home.dir", "/Users/dengjie/HDFS/hadoop-2.5.1");
	    Class.forName(DRIVER_HIVE);
	    connToHive = DriverManager.getConnection(SystemConfig.getProperty(envFlag + ".hive.db.url"), SystemConfig.getProperty(envFlag + ".hive.db.user"), SystemConfig.getProperty(envFlag + ".hive.db.password"));
	} catch (Exception e) {
	    logger.error("DBHelper.getHiveConn:error = " + e.getMessage());
	}
	return connToHive;
    }

    public static Connection getMySQLConn(String envFlag) {
	ComboPooledDataSource cpds = null;
	Connection conn = null;
	try {
	    cpds = new ComboPooledDataSource(SystemConfig.getProperty(envFlag + ".c3p0.mysql"));
	    conn = cpds.getConnection();
	} catch (Exception e) {
	    logger.error("DBHelper.getMySQLConn:error = " + e.getMessage());
	}
	return conn;
    }

    public static void closeHiveConn(Connection connToHive) {
	try {
	    if (connToHive != null) {
		connToHive.close();
	    }
	} catch (Exception e) {
	    logger.error("DBHelper.closeHiveConn:error = " + e.getMessage());
	}
    }

    public static void closeMySQLConn(Connection connToMySQL) {
	try {
	    if (connToMySQL != null) {
		connToMySQL.close();
	    }
	} catch (Exception e) {
	    logger.error("DBHelper.closeMySQLConn:error = " + e.getMessage());
	}
    }

}
