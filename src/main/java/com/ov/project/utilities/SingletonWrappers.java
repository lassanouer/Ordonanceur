package com.ov.project.utilities;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//Pas de synchronized pour des raisons de performance
public class SingletonWrappers {

	private static JavaSparkContext instanceJavaSparkContext;
	private static SparkConf instanceSparkConf;

	private SingletonWrappers() {
	}

	public static JavaSparkContext sparkContextGetInstance() {
		if (instanceJavaSparkContext == null) {
			instanceSparkConf = sparkConfGetInstance();
			instanceJavaSparkContext = new JavaSparkContext(instanceSparkConf);
		}
		return instanceJavaSparkContext;
	}

	public static SparkConf sparkConfGetInstance() {
		if (instanceSparkConf == null) {
			instanceSparkConf = new SparkConf().setAppName("OV Project").setMaster("local");
		}
		return instanceSparkConf;
	}

}
