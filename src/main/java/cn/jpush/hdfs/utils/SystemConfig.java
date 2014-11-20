package cn.jpush.hdfs.utils;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfig {
	private static Properties mConfig;

	private static Logger log = LoggerFactory.getLogger(SystemConfig.class);
	static {
		mConfig = new Properties();

		try {
			try {
				mConfig.load(SystemConfig.class.getClassLoader()
						.getResourceAsStream("hdfs.properties"));
			} catch (Exception exp1) {
				exp1.printStackTrace();
			}
			log.info("successfully loaded default properties.");

			// some debugging for those that want it
			if (log.isDebugEnabled()) {
				log.info("SystemConfig looks like this ...");

				String key = null;
				Enumeration keys = mConfig.keys();
				while (keys.hasMoreElements()) {
					key = (String) keys.nextElement();
					log.debug(key + "=" + mConfig.getProperty(key));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// no, you may not instantiate this class :p
	private SystemConfig() {
	}

	/**
	 * Retrieve a property value
	 * 
	 * @param key
	 *            Name of the property
	 * @return String Value of property requested, null if not found
	 */
	public static String getProperty(String key) {
		return mConfig.getProperty(key);
	}

	public static String getProperty(String key, String defaultValue) {
		log.debug("Fetching property [" + key + "=" + mConfig.getProperty(key)
				+ "]");

		String value = SystemConfig.getProperty(key);

		if (value == null) {
			return defaultValue;
		}

		return value;
	}

	/**
	 * Retrieve a property as a boolean ... defaults to false if not present.
	 */
	public static boolean getBooleanProperty(String name) {
		return getBooleanProperty(name, false);
	}

	/**
	 * Retrieve a property as a boolean ... with specified default if not
	 * present.
	 */
	public static boolean getBooleanProperty(String name, boolean defaultValue) {
		// get the value first, then convert
		String value = SystemConfig.getProperty(name);

		if (value == null) {
			return defaultValue;
		}
		return (new Boolean(value)).booleanValue();
	}

	/**
	 * Retrieve a property as a int ... defaults to 0 if not present.
	 * 
	 * @param name
	 * @return
	 */
	public static int getIntProperty(String name) {
		return getIntProperty(name, 0);
	}

	public static int getIntProperty(String name, int defaultValue) {
		// get the value first, then convert
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

	public static int[] getIntPropertyArray(String name, int[] defaultValue,
			String splitStr) {
		// get the value first, then convert
		String value = SystemConfig.getProperty(name);

		if (value == null) {
			return defaultValue;
		}

		try {
			String[] propertyArray = value.split(splitStr);// 灏嗗瓧绗︾敤閫楀紑鍒嗙
			int[] result = new int[propertyArray.length];
			for (int i = 0; i < propertyArray.length; i++) {//
				result[i] = Integer.parseInt(propertyArray[i]);
			}
			return result;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static boolean[] getBooleanPropertyArray(String name,
			boolean[] defaultValue, String splitStr) {
		// get the value first, then convert
		String value = SystemConfig.getProperty(name);
		if (value == null) {
			return defaultValue;
		}
		try {
			String[] propertyArray = value.split(splitStr);// 灏嗗瓧绗︾敤閫楀紑鍒嗙
			boolean[] result = new boolean[propertyArray.length];
			for (int i = 0; i < propertyArray.length; i++) {//
				result[i] = (new Boolean(propertyArray[i])).booleanValue();
			}
			return result;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static String[] getPropertyArray(String name, String[] defaultValue,
			String splitStr) {
		// get the value first, then convert
		String value = SystemConfig.getProperty(name);
		if (value == null) {
			return defaultValue;
		}
		try {
			String[] propertyArray = value.split(splitStr);// 灏嗗瓧绗︾敤閫楀紑鍒嗙
			return propertyArray;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	public static String[] getPropertyArray(String name, String splitStr) {
		// get the value first, then convert
		String value = SystemConfig.getProperty(name);
		if (value == null) {
			return null;
		}
		try {
			String[] propertyArray = value.split(splitStr);// 灏嗗瓧绗︾敤閫楀紑鍒嗙
			return propertyArray;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Retrieve all property keys
	 * 
	 * @return Enumeration A list of all keys
	 */
	public static Enumeration keys() {
		return mConfig.keys();
	}

	public static Map getPropertyMap(String name) {
		String[] maps = getPropertyArray(name, ",");
		Map map = new TreeMap();
		try {
			for (String str : maps) {
				String[] array = str.split(":");
				if (array.length > 1) {
					map.put(array[0], array[1]);
				}
			}
		} catch (Exception e) {
			log.error("获取PropertyMap信息错误! key is :" + name);
			e.printStackTrace();
		}

		return map;
	}
}
