package com.ov.project.dev.crawler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import com.ov.project.utilities.Constants;

public class ServerOVPrediGO {

	private ServerSocket mServer = null;
	private boolean mIsRunning = true;

	/**
	 * 
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

	// On lance notre serveur
	public void open() {

		// Toujours dans un thread à part vu qu'il est dans une boucle infinie
		Thread lThread = new Thread(new Runnable() {
			public void run() {
				while (mIsRunning == true) {

					try {
						// On attend une connexion d'un client
						Socket lSocket = mServer.accept();

						// Manu avec le client non encore achevée

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

	// test
	public static void main(String[] args) {
		ServerOVPrediGO lvlibCrawler = new ServerOVPrediGO();
		lvlibCrawler.open();

		// under Construction !!!

		// VelibAppli.start(true);
	}

}