/**
 * 
 */
package cn.jpush.hdfs.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.mr.domain.HBaseTableDomain;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class HBaseFactory {

    private static Logger log = LoggerFactory.getLogger(HBaseFactory.class);
    private static Configuration conf;
    static {
	conf = HBaseConfiguration.create();
	// conf.set("hbase.zookeeper.property.clientPort", "2888");
	conf.set("hbase.zookeeper.quorum", "10.211.55.12");
	// conf.set("master", "192.168.1.3:60010");
    }

    public static void create(String tableName, String[] columnFamily) {
	HBaseAdmin admin;
	try {
	    admin = new HBaseAdmin(conf);
	    if (admin.tableExists(tableName)) {
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		log.info(tableName + " is exist ,delete ......");
	    }
	    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
	    for (String col : columnFamily) {
		HColumnDescriptor hColDesc = new HColumnDescriptor(col);// 列族名
		tableDescriptor.addFamily(hColDesc);
	    }
	    admin.createTable(tableDescriptor);
	    log.info("create table[" + tableName + "] finished");
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error("create table is error -> " + e.getMessage());
	}
    }

    /**
     * drop table according to tableName
     * 
     * @param tableName
     */
    public static void drop(String tableName) {
	HBaseAdmin admin;
	try {
	    admin = new HBaseAdmin(conf);
	    if (admin.tableExists(tableName)) {
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		log.info(tableName + " delete success!");
	    } else {
		log.info(tableName + " table does not exist!");
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error("delete table is error -> " + e.getMessage());
	}
    }

    /**
     * insert into single data
     * 
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public static void insert(String tableName, String rowKey, String columnFamily, String column, String value) {
	try {
	    HBaseAdmin admin;
	    HTable table = new HTable(conf, tableName);
	    try {
		admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
		    Put put = new Put(Bytes.toBytes(rowKey));// 行键唯一
		    // 参数分别为：列族，列，值
		    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		    table.put(put);
		    log.info("add [" + tableName + "] success!");
		} else {
		    log.info(tableName + " table does not exist!");
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("insert into single data has error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("insert into single data has error -> " + ex.getMessage());
	}
    }

    /**
     * insert into ArrayList,ArrayList param is Put
     * 
     * @param tableName
     * @param alists
     */
    public static void insert(String tableName, ArrayList<Put> alists) {
	try {
	    HBaseAdmin admin;
	    HTable table = new HTable(conf, tableName);
	    try {
		admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
		    table.put(alists);
		    log.info("add [" + tableName + "] success!");
		} else {
		    log.info(tableName + " table does not exist!");
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("insert into ArrayList has error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("insert into ArrayList has error -> " + ex.getMessage());
	}
    }

    /**
     * delete from rowKey
     * 
     * @param tableName
     * @param rowKey
     */
    public static void delete(String tableName, String rowKey) {
	try {
	    HBaseAdmin admin;
	    HTable table = new HTable(conf, tableName);
	    try {
		admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
		    Delete delete = new Delete(Bytes.toBytes(rowKey));
		    table.delete(delete);
		    log.info("delete [" + tableName + "]success!");
		} else {
		    log.info(tableName + " table does not exist!");
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("delete from rowKey has error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    log.error("delete from rowKey has error -> " + ex.getMessage());
	}
    }

    /**
     * delete from rowKey String Array
     * 
     * @param tableName
     * @param rowKey
     */
    public static void delete(String tableName, String[] rowKey) {
	try {
	    HBaseAdmin admin;
	    HTable table = new HTable(conf, tableName);
	    try {
		admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
		    ArrayList<Delete> alist = new ArrayList<Delete>();
		    for (String key : rowKey) {
			Delete del = new Delete(Bytes.toBytes(key));
			alist.add(del);
		    }
		    table.delete(alist);
		    log.info("delete [" + tableName + "]success!");
		} else {
		    log.info(tableName + " table does not exist!");
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("delete from rowKey String Array has error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("delete from rowKey String Array has error -> " + ex.getMessage());
	}
    }

    /**
     * select tableName and return HBaseTableDomain Object
     * 
     * @param tableName
     * @param rowKey
     * @return
     */
    public static HBaseTableDomain get(String tableName, String rowKey) {
	HBaseTableDomain hTableDomain = new HBaseTableDomain();
	try {
	    HTable table = new HTable(conf, tableName);
	    try {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);

		for (Cell cell : result.rawCells()) {
		    hTableDomain.setRowName(new String(CellUtil.cloneRow(cell)));
		    hTableDomain.setColumnFamily(new String(CellUtil.cloneFamily(cell)));
		    hTableDomain.setColumnFamilyName(new String(CellUtil.cloneQualifier(cell)));
		    hTableDomain.setColumnFamilyValue(new String(CellUtil.cloneValue(cell)));
		    hTableDomain.setTimestamp(cell.getTimestamp());
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("get rowKey[" + rowKey + "] data is error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get rowKey[" + rowKey + "] data is error -> " + ex.getMessage());
	}
	return hTableDomain;
    }

    public static List<HBaseTableDomain> get(String tableName) {
	List<HBaseTableDomain> hTableDomainSet = new ArrayList<HBaseTableDomain>();
	try {
	    HTable table = new HTable(conf, tableName);
	    try {
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		for (Result result : results) {
		    for (Cell cell : result.rawCells()) {
			HBaseTableDomain hTableDomain = new HBaseTableDomain();
			hTableDomain.setRowName(new String(CellUtil.cloneRow(cell)));
			hTableDomain.setColumnFamily(new String(CellUtil.cloneFamily(cell)));
			hTableDomain.setColumnFamilyName(new String(CellUtil.cloneQualifier(cell)));
			hTableDomain.setColumnFamilyValue(new String(CellUtil.cloneValue(cell)));
			hTableDomain.setTimestamp(cell.getTimestamp());
			hTableDomainSet.add(hTableDomain);
		    }
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("get all data is error -> " + e.getMessage());
	    } finally {
		table.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get all data is error -> " + ex.getMessage());
	}
	return hTableDomainSet;
    }
}
