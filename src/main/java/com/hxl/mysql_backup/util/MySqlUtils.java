package com.hxl.mysql_backup.util;

public class MySqlUtils {

	/**
	 * 
	* @Title: escape 
	* @Description: 特殊字符转义 
	* @param @param old
	* @param @return 
	* @return String 
	* @throws
	 */
	public static String escape(String old) {
		return old.replace("\\", "\\\\").replace("'", "\\'");
	}
	
	
}
