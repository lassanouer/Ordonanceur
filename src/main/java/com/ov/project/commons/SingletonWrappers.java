package com.ov.project.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
// Pas de synchronized pour des raisons de performance
public class SingletonWrappers {

	private static JavaSparkContext mInstanceJavaSparkContext;
	private static SparkConf mInstanceSparkConf;

	private SingletonWrappers() {
	}

	public static JavaSparkContext sparkContextGetInstance() {
		if (mInstanceJavaSparkContext == null) {
			mInstanceSparkConf = sparkConfGetInstance();
			mInstanceJavaSparkContext = new JavaSparkContext(mInstanceSparkConf);
		}
		return mInstanceJavaSparkContext;
	}

	public static SparkConf sparkConfGetInstance() {
		if (mInstanceSparkConf == null) {
			mInstanceSparkConf = new SparkConf().setAppName("OV Project").setMaster("local");
		}
		return mInstanceSparkConf;
	}

}
