/**
 * 
 */
package cn.jpush.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengjie
 * @date 2014年12月8日
 * @description TODO
 */
public class CreateIPTest {

    private static Logger log = LoggerFactory.getLogger(CreateIPTest.class);

    public static void main(String[] args) {
	for (int i = 0; i < 255; i++) {
	    log.info("192.168.1." + i);
	}
    }

}
