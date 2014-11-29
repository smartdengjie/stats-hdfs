/**
 * 
 */
package cn.jpush.hdfs;

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
	new ChildClazz().walk();
    }
    
}
