package com.ov.project.dev.crawler;

import com.ov.SparkManager;
import com.ov.project.commons.DataManipulation;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.Constants;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
public class VelibCrawler {

	private boolean sCancel = false;

	/**
	 * Ceci thread qui aspire les données pour la station
	 * 
	 * @return
	 */
	public Thread goCrawlThread() {
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
						Thread.sleep(Constants.sDeuxMinutes);
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
	public void cancel() {
		sCancel = true;
	}

	/**
	 * start crawler
	 */
	public void start() {
		Thread lCrawlerThread = new Thread(goCrawlThread());
		lCrawlerThread.start();
	}

}
