/**
 * 
 */
package cn.jpush.hdfs.mr.hive;

import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.jpush.hdfs.utils.ConfigUtils;
import cn.jpush.hdfs.utils.HiveUtil;
import cn.jpush.hdfs.utils.SystemConfig;

/**
 * @author dengjie
 * @date 2014年12月4日
 * @description 参考HiveVisit，这个类暂时不推荐使用，过度封装了，适合于真实的业务场景。
 */
public class HiveOperateHDFS {

    private static Logger log = LoggerFactory.getLogger(HiveOperateHDFS.class);
    private static final String TABLENAME = "stu";
    private static final String ENV_FLAG = "test";

    public static void main(String[] args) {
	try {
	    String hdfsPath = String.format(ConfigUtils.HIVE.HDFS_FILE_PATH, TABLENAME + "/people_copy_1");
	    String sql = String.format(SystemConfig.getProperty(ConfigUtils.HIVE.LOAD_HDFS), hdfsPath, TABLENAME);
	    log.info("sql -> " + sql);
	    HiveUtil.execute(ENV_FLAG, sql);
	    String querySql = String.format(SystemConfig.getProperty(ConfigUtils.HIVE.QUERY), TABLENAME);
	    System.out.println(querySql);
	    ResultSet rs = HiveUtil.executeQuery(ENV_FLAG, querySql);
	    System.out.println(rs.next());
	} catch (Exception e) {
	    e.printStackTrace();
	    log.error("Query hive table has error , msg -> " + e.getMessage());
	}
    }
}
