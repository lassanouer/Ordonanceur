package com.ov.project.dev.crawler;

import com.ov.SparkManager;
import com.ov.project.commons.DataManipulation;
import com.ov.project.utilities.BundelUtils;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
public class VelibCrawler {

	private static boolean sCancel = false;
	private static final int sDeuxMinutes = 120000;

	/**
	 * Ceci thread qui aspirr les données pour la station fournie tous les 2
	 * minutes
	 * 
	 * @return
	 */
	private static Thread goCrawlThread() {
		return new Thread(new Runnable() {
			public void run() {
				while (!sCancel) {
					SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
					String lJsonFile;
					// get les données brutes de JcDecaux
					lJsonFile = DataManipulation.getDataBrute(BundelUtils.get("url.stations"),
							BundelUtils.get("bruteData.path"));

					// generate parquets
					DataManipulation.getParquets(lJsonFile, BundelUtils.get("data.frame.path"));

					try {
						Thread.sleep(sDeuxMinutes);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
	}

	/**
	 * Arrete le thread goCrawlThread().
	 */
	public static void cancel() {
		sCancel = true;
	}

	// test
	public static void main(String[] args) {
		Thread lCrawlerThread = new Thread(goCrawlThread());
		lCrawlerThread.start();
	}
}
