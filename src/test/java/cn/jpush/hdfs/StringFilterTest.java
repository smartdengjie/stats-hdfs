/**
 * 
 */
package cn.jpush.hdfs;

import java.util.StringTokenizer;

/**
 * @author dengjie
 * @date 2014年12月29日
 * @description TODO
 */
public class StringFilterTest {

    public static void main(String[] args) {
	String value = "a b c c d";
	StringTokenizer itr = new StringTokenizer(value.toString());
	while (itr.hasMoreTokens()) {
	    System.out.println(itr.nextToken());
	}
    }

}
