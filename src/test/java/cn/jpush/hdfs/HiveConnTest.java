package cn.jpush.hdfs;

import cn.jpush.hdfs.utils.DBHelper;

/**
 * @author dengjie
 * @date 2014年12月4日
 * @description TODO 
 */
public class HiveConnTest {
    
    public static void main(String[] args) {
//	DBHelper.getHiveConn("test");
	String s = "a'%s'%s";
	System.out.println(String.format(s, "ok","iii"));
    }

}
