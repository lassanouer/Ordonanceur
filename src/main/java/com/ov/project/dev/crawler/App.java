package com.ov.project.dev.crawler;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.ov.SparkManager;
import com.ov.VelibKey;
import com.ov.VelibProvider;

public class App {

	public static void main(String[] args) throws InterruptedException {
		
		String lDataPath = "C:/Velib/data/databruteDF/";
		String lStaticPath = "file:/C:/Velib/data/datastatic/staticspark";
		String lModelPath = "C:/Velib/data/model/";
		String lHadoopHome = "c:/Hadoop/";
		String lLicense = "C:/Velib/license.jpg";
		int lValue = 30;

		
		
		@SuppressWarnings("unused")
		Map<VelibKey, Integer> lMap = new HashMap<VelibKey, Integer>();
		try {
			SparkManager.getInstance().init(lHadoopHome);
			VelibProvider lProvider = new VelibProvider(lLicense);
			lMap = lProvider.predict(lDataPath, lStaticPath, lModelPath, lHadoopHome, Calendar.MINUTE, lValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
