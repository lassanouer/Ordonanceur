package com.ov.project.dev.crawler;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.glassfish.grizzly.http.util.TimeStamp;

import com.ov.SparkManager;
import com.ov.VelibAppli;
import com.ov.VelibKey;
import com.ov.VelibProvider;
import com.ov.VelibStation;
import com.ov.geo.Prediction;
import com.ov.project.commons.SingletonWrappers;
import com.ov.project.utilities.BundelUtils;

import scala.Tuple2;

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

	public Map<VelibKey, Integer> simuleProvider() {
		HashMap<VelibKey, Integer> result = new HashMap<>();

		long ts = new TimeStamp().getCreationTime();

		VelibKey tempKey = new VelibKey("42208", "OPEN", ts);
		result.put(tempKey, 2);

		// VelibClient client = new VelibClient(Constants.sHost,
		// Constants.sPort, lMap);

		return result;
	}

	public Map<VelibStation, Prediction> jobToDo(Map<VelibKey, Integer> iDataProvided) {

		System.setProperty("hadoop.home.dir", BundelUtils.get("hadoop.home"));

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext lSparkctx = SingletonWrappers.sparkContextGetInstance();

		Job lJob;

		try {
			lJob = Job.getInstance();
			FileInputFormat.setInputPaths(lJob, new Path(BundelUtils.get("data.frame.path")));
			FileInputFormat.setInputDirRecursive(lJob, true);
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		
		Prediction predi = new Prediction();

	//	com.ov.PredictionsBuilder.runPredictions(BundelUtils.get("bruteData.path"), BundelUtils.get("static.path"), BundelUtils.get("output.path"), BundelUtils.get("model.path"), BundelUtils.get("hadoop.home"), iSave, Calendar.MINUTE, new TimeStamp(), new TimeStamp(), BundelUtils.get("license.path"));
		
	//	JavaRDD<Text> sourceData = lSparkctx
	//			.newAPIHadoopRDD(lJob.getConfiguration(), TextInputFormat.class, LongWritable.class, Text.class)
	//			.values();

		// Each line will be translate to a session defined by the IP adress
	//	JavaPairRDD<VelibStation, Prediction> lsession = sourceData
	//			.mapToPair(
	//					w -> new Tuple2<VelibStation, Prediction>(LogParser.getFirstToken(w), LogParser.parseTokenz(w)))
	//			.reduceByKey((a, b) -> reduceByIP(a, b));

		// Save the word count back out to a text file, causing evaluation.
//		FileUtils.deleteQuietly(new File(BundelUtils.get("suffix.for.result.file")));
//		lsession.saveAsTextFile(BundelUtils.get("suffix.for.result.file"));

		return null;
	}

}
