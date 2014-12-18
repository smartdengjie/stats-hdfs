package cn.jpush.hdfs;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import cn.jpush.hdfs.utils.SystemConfig;

public class SystemTest {

    public static void main(String[] args) {
//	test();
	Gson gon = new Gson();
	Map<String, Object> ext = new HashMap<String, Object>();
	Map<String, Object> ext2 = new HashMap<String, Object>();
	ext.put("sound", "default");
	ext.put("badge", "+1");
	ext2.put("ios", ext);
	System.out.println(gon.toJson(ext2));
    }

    private static void test() {
	String max = SystemConfig.getProperty("max-count");
	for (int i = 1; i < Integer.parseInt(max); i++) {
	    System.out.println(i + "," + SystemConfig.getProperty(i + ""));
	}
    }

}
