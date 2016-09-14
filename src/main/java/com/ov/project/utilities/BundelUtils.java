package com.ov.project.utilities;

import java.util.ResourceBundle;

/**
 * 
 * @author anouer.lassoued
 *
 */
public class BundelUtils {
	private BundelUtils() {
		super();
	}

	public static final String SYS_CONFIG = "config";

	/**
	 * Gets the param.
	 *
	 * @param bundleName
	 *            the bundle name
	 * @param key
	 *            the key
	 * @return the param
	 */
	public static String get(String key) {
		try {
			ResourceBundle bundle = ResourceBundle.getBundle(SYS_CONFIG);

			String ret = bundle.getString(key);

			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return key;
		}
	}

}