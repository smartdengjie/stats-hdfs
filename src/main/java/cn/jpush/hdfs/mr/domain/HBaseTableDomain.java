package cn.jpush.hdfs.mr.domain;

import com.google.gson.Gson;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class HBaseTableDomain {

    private String rowName;
    private long timestamp;
    private String columnFamily;
    private String columnFamilyName;
    private String columnFamilyValue;

    /**
     * @return the rowName
     */
    public String getRowName() {
	return rowName;
    }

    /**
     * @param rowName
     *            the rowName to set
     */
    public void setRowName(String rowName) {
	this.rowName = rowName;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
	return timestamp;
    }

    /**
     * @param timestamp
     *            the timestamp to set
     */
    public void setTimestamp(long timestamp) {
	this.timestamp = timestamp;
    }

    /**
     * @return the columnFamily
     */
    public String getColumnFamily() {
	return columnFamily;
    }

    /**
     * @param columnFamily
     *            the columnFamily to set
     */
    public void setColumnFamily(String columnFamily) {
	this.columnFamily = columnFamily;
    }

    /**
     * @return the columnFamilyName
     */
    public String getColumnFamilyName() {
	return columnFamilyName;
    }

    /**
     * @param columnFamilyName
     *            the columnFamilyName to set
     */
    public void setColumnFamilyName(String columnFamilyName) {
	this.columnFamilyName = columnFamilyName;
    }

    /**
     * @return the columnFamilyValue
     */
    public String getColumnFamilyValue() {
	return columnFamilyValue;
    }

    /**
     * @param columnFamilyValue
     *            the columnFamilyValue to set
     */
    public void setColumnFamilyValue(String columnFamilyValue) {
	this.columnFamilyValue = columnFamilyValue;
    }

    /**
     * override class to json
     */
    @Override
    public String toString() {
	return new Gson().toJson(this);
    }

}
