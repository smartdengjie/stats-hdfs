/**
 * 
 */
package cn.jpush.hdfs.utils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO 
 */
public class HDFSCountErrorUtils extends Configured implements Tool{

    public int run(String[] args) throws Exception {
	// TODO Auto-generated method stub
	return 0;
    }
    
    public enum COUNTER{
	LINESKIP // 出错的行数
    }

}
