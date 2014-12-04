package cn.jpush.hdfs.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static ResultSet executeQuery(String envFlag, String sql) {
	long startTime = System.currentTimeMillis();
	logger.info("HiveUtil.execute:sql =" + sql);
	ResultSet res = null;
	Connection conn = null;
	try {
	    conn = DBHelper.getHiveConn(envFlag);
	    HiveStatement stmt = (HiveStatement) conn.createStatement();
	    res = stmt.executeQuery(sql);
	} catch (Exception e) {
	    logger.error("HiveUtil.executeQuery:error = " + e.getMessage());
	} finally {
	    DBHelper.closeHiveConn(conn);
	}
	logger.debug("HiveUtil.executeQuery:all time =" + (System.currentTimeMillis() - startTime));
	return res;
    }

    public static int executeUpdate(String envFlag, String sql) {
	long startTime = System.currentTimeMillis();
	logger.info("HiveUtil.execute:sql =" + sql);
	int res = 0;
	Connection conn = null;
	try {
	    conn = DBHelper.getHiveConn(envFlag);
	    Statement stmt = conn.createStatement();
	    res = stmt.executeUpdate(sql);
	} catch (Exception e) {
	    logger.error("HiveUtil.executeUpdate:error = " + e.getMessage());
	} finally {
	    DBHelper.closeHiveConn(conn);
	}
	logger.debug("HiveUtil.executeUpdate:all time =" + (System.currentTimeMillis() - startTime));
	return res;
    }

    public static void execute(String envFlag, String sql) throws Exception {
	long startTime = System.currentTimeMillis();
	logger.info("HiveUtil.execute:sql =" + sql);
	Connection conn = null;
	try {
	    conn = DBHelper.getHiveConn(envFlag);
	    Statement stmt = conn.createStatement();
	    stmt.execute(sql);
	} catch (Exception e) {
	    logger.error("HiveUtil.execute:error = " + e.getMessage());
	    e.printStackTrace();
	    throw new Exception(e);
	} finally {
	    DBHelper.closeHiveConn(conn);
	}
	logger.info("HiveUtil.execute:all time =" + (System.currentTimeMillis() - startTime));
    }

}
