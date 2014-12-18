/**
 * 
 */
package cn.jpush.hdfs.mr.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import cn.jpush.hdfs.mr.domain.HBaseTableDomain;
import cn.jpush.hdfs.utils.HBaseFactory;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class HBaseExample {

    @Test
    public void listTable() {
	System.out.println(HBaseFactory.list().toString());
    }

    @Test
    public void createTable() {
	HBaseFactory.create("ads", new String[] { "catalog" });
    }

    @Test
    public void insertTable() {
	HBaseFactory.insert("ads", "20141211", "catalog", "size", "80cm");
    }

    @Test
    public void getTableDetail(){
	System.out.println(HBaseFactory.get("ads"));
    }
    
    @Test
    public void getTable() {
	try {
	    SimpleDateFormat startFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
	    SimpleDateFormat endFormat = new SimpleDateFormat("yyyy-MM-dd 23:59:59");
	    SimpleDateFormat tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    long start = tmp.parse(startFormat.format(new Date())).getTime();
	    long end = tmp.parse(endFormat.format(new Date())).getTime();
	    System.out.println(start + "," + end);
	    System.out.println(HBaseFactory.get("ads", start, end).toString());
	} catch (Exception ex) {
	    ex.printStackTrace();
	}

    }

    @Test
    public void version() {
	System.out.println(HBaseFactory.version());
    }

    @Test
    public void getColumFamily() {
	List<HBaseTableDomain> list = HBaseFactory.get("people");
	for (HBaseTableDomain hBaseTableDomain : list) {
	    System.out.println(hBaseTableDomain.getRowName() + " " + hBaseTableDomain.getColumnFamily() + " " + hBaseTableDomain.getColumnFamilyName() + " " + hBaseTableDomain.getColumnFamilyValue() + " " + hBaseTableDomain.getTimestamp());
	}
    }
}
