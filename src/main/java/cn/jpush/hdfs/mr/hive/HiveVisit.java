/**
 * 
 */
package cn.jpush.hdfs.mr.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author dengjie
 * @date 2014年12月4日
 * @description 提供一个JDBC访问hive的原型，若用在实际业务中，可拓展该类。
 */
public class HiveVisit {

    static {
	// 注册jdbc驱动
	try {
	    Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) throws Exception {
	// 创建连接
	System.setProperty("hadoop.home.dir", "/Users/dengjie/HDFS/hadoop-2.5.1");
	Connection conn = DriverManager.getConnection("jdbc:hive://10.211.55.12:10000/default", "", "");
	Statement st = conn.createStatement();
	String tableName = "stu";
	// 删除表
	st.executeQuery("drop table " + tableName);
	// 创建表
	ResultSet rs = st.executeQuery("create table " + tableName + "(" + "id string," + "name string," + "sex string" + ")" + "row format delimited " + "fields terminated by ',' " + "stored as textfile");
	// 显示所有的表
	String sql = "show tables";
	System.out.println("running:" + sql);
	rs = st.executeQuery(sql);
	if (rs.next()) {
	    System.out.println(rs.getString(1));
	}
	// 得到表信息
	sql = "describe " + tableName;
	System.out.println("running:" + sql);
	rs = st.executeQuery(sql);
	while (rs.next()) {
	    System.out.println(rs.getString(1) + "\t" + rs.getString(2));
	}
	// 加载hdfs的数据
//	String filePath = "hdfs://10.211.55.12:9000/home/hive/warehouse/stu/people";
//	sql = "load data inpath '" + filePath + "' overwrite into table " + tableName;

	// 加载本地的数据
	sql = "load data local inpath '/home/cloud001/hdfs/people' overwrite into table " + tableName;
	System.out.println("running:" + sql);
	rs = st.executeQuery(sql);
	// 查询数据
	sql = "select id as id,name as name,sex as sex from " + tableName + " limit 5";
	System.out.println("running:" + sql);
	rs = st.executeQuery(sql);
	while (rs.next()) {
	    System.out.println(rs.getString("id") + "," + rs.getString("name") + "," + rs.getString("sex"));
	}
	// 查询数量
	sql = "select count(*) as count from " + tableName;
	System.out.println("running:" + sql);
	rs = st.executeQuery(sql);
	while (rs.next()) {
	    System.out.println(rs.getString("count"));
	}
	// 关闭资源
	rs.close();
	st.close();
	conn.close();
    }

}
