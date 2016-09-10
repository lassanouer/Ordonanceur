package com.ov.project.dev.crawler;

import com.ov.SparkManager;
import com.ov.VelibProvider;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.DataManipulation;

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
					String jsonFile;
					// get les données brutes de JcDecaux
					jsonFile = DataManipulation.getDataBrute(BundelUtils.get("url.stations"),
							BundelUtils.get("bruteData.path"));

					// generate parquets
					DataManipulation.getParquets(jsonFile, BundelUtils.get("data.frame.path"));

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
	 * Arrete l'application et tous ses threads.
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
