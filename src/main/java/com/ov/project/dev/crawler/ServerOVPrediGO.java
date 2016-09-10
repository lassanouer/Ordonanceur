package com.ov.project.dev.crawler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import com.ov.project.utilities.Constants;

public class ServerOVPrediGO {

	/*
	 * Variables de test
	 */
	int lValue = 30;
	private ServerSocket server = null;
	private boolean isRunning = true;
	/*
	 * 
	 */

	public ServerOVPrediGO() {
		try {
			server = new ServerSocket(Constants.sPort, 100, InetAddress.getByName(Constants.sHost));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// On lance notre serveur
	public void open() {

		// Toujours dans un thread à part vu qu'il est dans une boucle infinie
		Thread t = new Thread(new Runnable() {
			public void run() {
				while (isRunning == true) {

					try {
						// On attend une connexion d'un client
						Socket socket = server.accept();

						// Manu avec le client non encore achevée

						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				try {
					server.close();
				} catch (IOException e) {
					e.printStackTrace();
					server = null;
				}
			}
		});

		t.start();
	}

	public void close() {
		isRunning = false;
	}

	// test
	public static void main(String[] args) {
		ServerOVPrediGO lvlibCrawler = new ServerOVPrediGO();
		lvlibCrawler.open();

		// under Construction !!!

		// VelibAppli.start(true);
	}

}