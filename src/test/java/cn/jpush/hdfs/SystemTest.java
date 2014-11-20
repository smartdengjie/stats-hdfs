package cn.jpush.hdfs;

import cn.jpush.hdfs.utils.SystemConfig;

public class SystemTest {
	
	public static void main(String[] args) {
		String max = SystemConfig.getProperty("max-count");
		for(int i=1;i<Integer.parseInt(max);i++){
			System.out.println(i+","+SystemConfig.getProperty(i+""));
		}
	}

}
