/**
 * 
 */
package cn.jpush.hdfs.mr.hbase;

import java.util.List;

import cn.jpush.hdfs.mr.domain.HBaseTableDomain;
import cn.jpush.hdfs.utils.HBaseFactory;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class HBaseExample {

    public static void main(String[] args) {

	List<HBaseTableDomain> set = HBaseFactory.get("test");
//	System.out.println(set.toString());
	System.out.println(HBaseFactory.get("test","row1").toString());
    }

}
