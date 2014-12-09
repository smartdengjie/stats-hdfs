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
    public void printTableList(){
	System.out.println(HBaseFactory.list().toString());
    }
    
    @Test
    public void createTable(){
	HBaseFactory.create("people", new String[]{"info","member"});
    }
    
    @Test
    public void insertTable(){
	HBaseFactory.insert("people", "001", "info", "sex", "m");
    }
    
    @Test
    public void getTable(){
	System.out.println(HBaseFactory.get("people","001","info","name").toString());
    }

}
