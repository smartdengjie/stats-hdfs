/**
 * 
 */
package cn.jpush.hdfs.mr.hbase;

import org.junit.Test;

import cn.jpush.hdfs.utils.HBaseFactory;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class HBaseExample {

    public static void main(String[] args) {
	System.out.println(HBaseFactory.get("test","row1").toString());
    }
    
    public void printAll(){
	
    }
    
    @Test
    public void printTableList(){
	System.out.println(HBaseFactory.list().toString());
    }
    
    @Test
    public void createTable(){
	HBaseFactory.create("stu", new String[]{""});
    }

}
