/**
 * 
 */
package cn.jpush.hdfs.mr.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author dengjie
 * @date 2014年12月7日
 * @description UDF函数（加法）
 */
public class HiveUdfAdd extends UDF {

    public Integer evaluate(Integer a, Integer b) {
	if (null == a || null == b) {
	    return null;
	}
	return a + b;
    }

    public Double evaluate(Double a, Double b) {
	if (null == a || null == b) {
	    return null;
	}
	return a + b;
    }
    
    public Integer evaluate(Integer... a){
	int total =0;
	for(int i=0;i<a.length;i++){
	    if(a[i]!=null){
		total+=a[i];
	    }
	}
	return total;
    }

}
