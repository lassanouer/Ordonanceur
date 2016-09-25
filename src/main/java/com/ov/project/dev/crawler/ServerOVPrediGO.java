package com.ov.project.dev.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import com.ov.project.mapper.PredictsByStationDTO;
import com.ov.project.utilities.Constants;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
public class ServerOVPrediGO {

	private ServerSocket mServer = null;
	private boolean mIsRunning = true;

	/*
	 * Constructor
	 */
	public ServerOVPrediGO() {
		try {
			mServer = new ServerSocket(Constants.sPort, 100, InetAddress.getByName(Constants.sHost));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * Thread de reception appel aux prédiction de clients
	 */
	public void open() {
		// Toujours dans un thread à part vu qu'il est dans une boucle infinie
		Thread lThread = new Thread(new Runnable() {
			public void run() {
				while (mIsRunning == true) {

					try {
						// On attend une connexion d'un client
						Socket lSocket = mServer.accept();
						BufferedReader reader = new BufferedReader(new InputStreamReader(lSocket.getInputStream()));
						PrintStream output = new PrintStream(lSocket.getOutputStream());
						// String lOutputLigne;
						ChronsPredict lchronsPredict = new ChronsPredict();
						
						//get hourly predict, 
						lchronsPredict.Start();
						
						// add to map 4 pred of 30min model and 5 préd from  Daily models
						// 
						
						for (Map.Entry<String, PredictsByStationDTO> entry : lchronsPredict.getMpredictMap()
								.entrySet()) {
							output.append(entry.getValue().toString()).append("\n");
						}

						reader.close();
						lSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				try {
					mServer.close();
				} catch (IOException e) {
					e.printStackTrace();
					mServer = null;
				}
			}
		});

		lThread.start();
	}

	public void close() {
		mIsRunning = false;
	}

}