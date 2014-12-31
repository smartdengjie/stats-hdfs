/**
 * 
 */
package cn.jpush.hdfs;

/**
 * @author dengjie
 * @date 2014年12月30日
 * @description TODO
 */
public class TestException {

    public static void main(String[] args) {
	System.out.println(test(1));
	System.out.println("ok");
    }

    public static int test(int a) {
	int i = 0;
	try {
	    if (a == Integer.parseInt("a")) {
		i++;
	    } else {
		i--;
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	}
	return i;
    }

}
