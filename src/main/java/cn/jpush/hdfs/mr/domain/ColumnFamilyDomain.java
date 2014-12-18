/**
 * 
 */
package cn.jpush.hdfs.mr.domain;

import java.util.List;

import com.google.gson.Gson;

/**
 * @author dengjie
 * @date 2014年12月12日
 * @description TODO
 */
public class ColumnFamilyDomain {

    private String columnFamily;
    private List<String> coliqualifier;

    public String getColumnFamily() {
	return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
	this.columnFamily = columnFamily;
    }

    public List<String> getColiqualifier() {
	return coliqualifier;
    }

    public void setColiqualifier(List<String> coliqualifier) {
	this.coliqualifier = coliqualifier;
    }

    @Override
    public String toString() {
	return new Gson().toJson(this);
    }

}
