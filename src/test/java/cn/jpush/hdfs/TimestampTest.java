/**
 * 
 */
package cn.jpush.hdfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author dengjie
 * @date 2014年12月11日
 * @description TODO 
 */
public class TimestampTest {

    public static void main(String[] args) throws ParseException {
	SimpleDateFormat start = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
	SimpleDateFormat end = new SimpleDateFormat("yyyy-MM-dd 23:59:59");
	SimpleDateFormat tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	System.out.println(tmp.parse(start.format(new Date())).getTime());
	System.out.println(tmp.parse(end.format(new Date())).getTime());
	System.out.println(end.format(new Date(1417652765993L)));
    }
    
}
