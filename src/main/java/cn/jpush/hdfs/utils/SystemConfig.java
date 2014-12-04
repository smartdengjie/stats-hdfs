package cn.jpush.hdfs.utils;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author dengjie
 * @date 2014年12月2日
 * @description 操作property文件的工具类
 */
public class SystemConfig {
    private static Properties prop;

    private static Log log = LogFactory.getLog(SystemConfig.class);
    static {
	prop = new Properties();
	try {
	    try {
		prop.load(SystemConfig.class.getClassLoader().getResourceAsStream("system-config.properties"));
		prop.load(SystemConfig.class.getClassLoader().getResourceAsStream("jdbc.properties"));
	    } catch (Exception exp) {
		exp.printStackTrace();
	    }
	    log.info("successfully loaded default properties.");
	    if (log.isDebugEnabled()) {
		log.info("SystemConfig looks like this ...");
		String key = null;
		Enumeration<Object> keys = prop.keys();
		while (keys.hasMoreElements()) {
		    key = (String) keys.nextElement();
		    log.debug(key + "=" + prop.getProperty(key));
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    private SystemConfig() {
    }

    public static String getProperty(String key) {
	return prop.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
	log.debug("Fetching property [" + key + "=" + prop.getProperty(key) + "]");
	String value = SystemConfig.getProperty(key);
	if (value == null) {
	    return defaultValue;
	}
	return value;
    }

    public static boolean getBooleanProperty(String name) {
	return getBooleanProperty(name, false);
    }

    public static boolean getBooleanProperty(String name, boolean defaultValue) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return defaultValue;
	}
	return (new Boolean(value)).booleanValue();
    }

    public static int getIntProperty(String name) {
	return getIntProperty(name, 0);
    }

    public static int getIntProperty(String name, int defaultValue) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return defaultValue;
	}
	try {
	    return Integer.parseInt(value);
	} catch (NumberFormatException e) {
	    return defaultValue;
	}
    }

    public static int[] getIntPropertyArray(String name, int[] defaultValue, String splitStr) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return defaultValue;
	}
	try {
	    String[] propertyArray = value.split(splitStr);
	    int[] result = new int[propertyArray.length];
	    for (int i = 0; i < propertyArray.length; i++) {
		result[i] = Integer.parseInt(propertyArray[i]);
	    }
	    return result;
	} catch (NumberFormatException e) {
	    return defaultValue;
	}
    }

    public static boolean[] getBooleanPropertyArray(String name, boolean[] defaultValue, String splitStr) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return defaultValue;
	}
	try {
	    String[] propertyArray = value.split(splitStr);
	    boolean[] result = new boolean[propertyArray.length];
	    for (int i = 0; i < propertyArray.length; i++) {
		result[i] = (new Boolean(propertyArray[i])).booleanValue();
	    }
	    return result;
	} catch (NumberFormatException e) {
	    return defaultValue;
	}
    }

    public static String[] getPropertyArray(String name, String[] defaultValue, String splitStr) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return defaultValue;
	}
	try {
	    String[] propertyArray = value.split(splitStr);
	    return propertyArray;
	} catch (NumberFormatException e) {
	    return defaultValue;
	}
    }

    public static String[] getPropertyArray(String name, String splitStr) {
	String value = SystemConfig.getProperty(name);
	if (value == null) {
	    return null;
	}
	try {
	    String[] propertyArray = value.split(splitStr);
	    return propertyArray;
	} catch (NumberFormatException e) {
	    return null;
	}
    }

    public static Enumeration<Object> keys() {
	return prop.keys();
    }

    public static Map<String, String> getPropertyMap(String name) {
	String[] maps = getPropertyArray(name, ",");
	Map<String, String> map = new TreeMap<String, String>();
	try {
	    for (String str : maps) {
		String[] array = str.split(":");
		if (array.length > 1) {
		    map.put(array[0], array[1]);
		}
	    }
	} catch (Exception e) {
	    log.error("Get PropertyMap info error! key is :" + name);
	    e.printStackTrace();
	}
	return map;
    }
}
