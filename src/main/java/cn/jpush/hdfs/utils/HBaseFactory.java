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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.mr.domain.HBaseTableDomain;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description java操作hbase的API接口
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

    /**
     * create hbase table and init column family
     * 
     * @param tableName
     * @param columnFamily
     */
    public static void create(String tableName, String[] columnFamily) {
	try {
	    HBaseAdmin admin = new HBaseAdmin(conf);

	    try {
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
	    } finally {
		admin.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("create table is error -> " + ex.getMessage());
	}
    }

    /**
     * drop table according to tableName
     * 
     * @param tableName
     */
    public static void drop(String tableName) {
	try {
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    try {
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
	    } finally {
		admin.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("delete table is error -> " + ex.getMessage());
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
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		if (connection.isTableAvailable(TableName.valueOf(tableName))) {
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
		connection.close();
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
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		if (connection.isTableAvailable(TableName.valueOf(tableName))) {
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
		connection.close();
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
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		if (connection.isTableAvailable(TableName.valueOf(tableName))) {
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
		connection.close();
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
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		if (connection.isTableAvailable(TableName.valueOf(tableName))) {
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
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("delete from rowKey String Array has error -> " + ex.getMessage());
	}
    }

    /**
     * select tableName according to rowKey and return HBaseTableDomain Array
     * 
     * @param tableName
     * @param rowKey
     * @return
     */
    public static List<HBaseTableDomain> get(String tableName, String rowKey) {
	List<HBaseTableDomain> hTableDomainSet = new ArrayList<HBaseTableDomain>();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);

		for (Cell cell : result.rawCells()) {
		    HBaseTableDomain hTableDomain = new HBaseTableDomain();
		    hTableDomain.setRowName(new String(CellUtil.cloneRow(cell)));
		    hTableDomain.setColumnFamily(new String(CellUtil.cloneFamily(cell)));
		    hTableDomain.setColumnFamilyName(new String(CellUtil.cloneQualifier(cell)));
		    hTableDomain.setColumnFamilyValue(new String(CellUtil.cloneValue(cell)));
		    hTableDomain.setTimestamp(cell.getTimestamp());
		    hTableDomainSet.add(hTableDomain);
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("get rowKey[" + rowKey + "] data is error -> " + e.getMessage());
	    } finally {
		table.close();
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get rowKey[" + rowKey + "] data is error -> " + ex.getMessage());
	}
	return hTableDomainSet;
    }

    /**
     * select tableName according to rowKey and columnFamily, return
     * HBaseTableDomain Array
     * 
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @return
     */
    public static List<HBaseTableDomain> get(String tableName, String rowKey, String columnFamily) {
	List<HBaseTableDomain> hTableDomainSet = new ArrayList<HBaseTableDomain>();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(columnFamily.getBytes());
		Result result = table.get(get);

		for (Cell cell : result.rawCells()) {
		    HBaseTableDomain hTableDomain = new HBaseTableDomain();
		    hTableDomain.setRowName(new String(CellUtil.cloneRow(cell)));
		    hTableDomain.setColumnFamily(new String(CellUtil.cloneFamily(cell)));
		    hTableDomain.setColumnFamilyName(new String(CellUtil.cloneQualifier(cell)));
		    hTableDomain.setColumnFamilyValue(new String(CellUtil.cloneValue(cell)));
		    hTableDomain.setTimestamp(cell.getTimestamp());
		    hTableDomainSet.add(hTableDomain);
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("get rowKey[" + rowKey + "] data is error -> " + e.getMessage());
	    } finally {
		table.close();
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get rowKey[" + rowKey + "] data is error -> " + ex.getMessage());
	}
	return hTableDomainSet;
    }

    /**
     * select tableName according to rowKey,columnFamily,qulifier and return
     * HBaseTableDomain Object
     * 
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     * @return
     */
    public static HBaseTableDomain get(String tableName, String rowKey, String columnFamily, String qualifier) {
	HBaseTableDomain hTableDomain = new HBaseTableDomain();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(columnFamily.getBytes(), qualifier.getBytes());
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
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get rowKey[" + rowKey + "] data is error -> " + ex.getMessage());
	}
	return hTableDomain;
    }

    /**
     * select tableName get all record
     * 
     * @param tableName
     * @return
     */
    public static List<HBaseTableDomain> get(String tableName) {
	List<HBaseTableDomain> hTableDomainSet = new ArrayList<HBaseTableDomain>();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
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
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get all data is error -> " + ex.getMessage());
	}
	return hTableDomainSet;
    }

    /**
     * select htable range time
     * 
     * @param tableName
     * @param filterNumber
     * @return
     */
    public static List<HBaseTableDomain> get(String tableName, long start, long end) {
	List<HBaseTableDomain> hTableDomainSet = new ArrayList<HBaseTableDomain>();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    HTableInterface table = connection.getTable(tableName);
	    try {
		Scan scan = new Scan();
		scan.setTimeRange(start, end);
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
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("get all data is error -> " + ex.getMessage());
	}
	return hTableDomainSet;
    }

    /**
     * list hbase table name
     * 
     * @return
     */
    public static List<String> list() {
	List<String> list = new ArrayList<String>();
	try {
	    HConnection connection = HConnectionManager.createConnection(conf);
	    try {
		TableName[] tableList = connection.listTableNames();
		for (TableName tableName : tableList) {
		    list.add(tableName.getNameAsString());
		}
	    } catch (Exception e) {
		e.printStackTrace();
		log.error("list hbase table name has error -> " + e.getMessage());
	    } finally {
		connection.close();
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	    log.error("list hbase table name has error -> " + ex.getMessage());

	}
	return list;
    }

    /**
     * get hbase version
     * 
     * @return
     */
    public static String version() {
	return VersionInfo.getVersion();
    }

}
