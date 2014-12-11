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

    @Test
    public void listTable(){
	System.out.println(HBaseFactory.list().toString());
    }
    
    @Test
    public void createTable(){
	HBaseFactory.create("ads", new String[]{"catalog"});
    }
    
    @Test
    public void insertTable(){
	HBaseFactory.insert("ads", "20141211", "catalog", "type", "food");
    }
    
    @Test
    public void getTable(){
	System.out.println(HBaseFactory.get("people").toString());
    }

    @Test
    public void version(){
	System.out.println(HBaseFactory.version());
    }
    
}
