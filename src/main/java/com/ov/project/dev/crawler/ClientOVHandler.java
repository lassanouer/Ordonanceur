package com.ov.project.dev.crawler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.ov.SparkManager;
import com.ov.VelibKey;
import com.ov.VelibProvider;
import com.ov.project.utilities.BundelUtils;

public class ClientOVHandler {

	protected Socket client;
	protected PrintWriter out;

	private int lValue = 30;
	private boolean mIsRunning = true;

	public ClientOVHandler(Socket client) {
		this.client = client;
		try {
			this.out = new PrintWriter(client.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
						// predict throw l'exception de cast de VectorAssembler
						// a OneHotEncoder
						lMap = lProvider.predict(BundelUtils.get("data.frame.path"), BundelUtils.get("static.path"), BundelUtils.get("model.path"),
								BundelUtils.get("hadoop.home"), Calendar.MINUTE, lValue);

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		lThread.start();
	}

	/**
	 * stop thread
	 */
	public void close() {
		mIsRunning = false;
	}

}
