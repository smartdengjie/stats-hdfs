/**
 * 
 */
package cn.jpush.hdfs;

import java.util.StringTokenizer;

/**
 * @author dengjie
 * @date 2014年11月29日
 * @description TODO 
 */
public class ChildClazz extends BaseClazz{

    /* (non-Javadoc)
     * @see cn.jpush.hdfs.BaseClazz#walk()
     */
    @Override
    public void walk() {
	// TODO Auto-generated method stub
	System.out.println("walk child");
    }

    public static void main(String[] args) {
//	new ChildClazz().walk();
	String s = "ab c d e f";
	StringTokenizer token = new StringTokenizer(s, "\n");
	while (token.hasMoreElements()) {
	    StringTokenizer t = new StringTokenizer(token.nextToken());
	    String one = t.nextToken();
	    String two = t.nextToken();
	    System.out.println(one+","+two);
	}
    }
    
}
