package com.ov.project.dev.crawler;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.ov.SparkManager;
import com.ov.VelibAppli;
import com.ov.VelibClient;
import com.ov.VelibKey;
import com.ov.VelibProvider;
import com.ov.VelibStation;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.Constants;
import com.ov.transformTimestampData.TsRounderTransformer;

public class ClientOVSocket {

	private int lValue = 30;
	private boolean mIsRunning = true;

	/**
	 * 
	 * Thread d' appel aux prédiction
	 */
	public void open() {
		Thread lThread = new Thread(new Runnable() {
			public void run() {
				while (mIsRunning == true) {

					Map<VelibKey, Integer> lMap = new HashMap<VelibKey, Integer>();
					try {
						SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
						VelibProvider lProvider = new VelibProvider(BundelUtils.get("license.path"));
						lMap = lProvider.predict(BundelUtils.get("data.frame.path"), BundelUtils.get("static.path"),
								BundelUtils.get("model.path"), BundelUtils.get("hadoop.home"), Calendar.MINUTE, lValue);

						// Map => <VelibStation, Prediction>

						// VelibStation s = new VelibStation(iId);
						// com.ov.transformTimestampData.TsRounderTransformer n
						// = new TsRounderTransformer()

						// VelibClient client = new VelibClient(Constants.sHost,
						// Constants.sPort, lMap);

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		lThread.start();
	}

	public void close() {
		mIsRunning = false;
	}

}
