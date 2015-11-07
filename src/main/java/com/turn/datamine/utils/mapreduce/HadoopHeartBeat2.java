/**
 * Copyright (C) 2014-2015 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */
package com.turn.datamine.utils.mapreduce;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * The class sets up a thread to send heart beat regularly to the jobtracker. 
 *
 * @author Yan Qi
 */
public class HadoopHeartBeat2 {
	private Timer timer = null;
	private long delay = 5000; 
	private long interval = 5000;

	public void startHeartbeat(@SuppressWarnings("rawtypes") final TaskInputOutputContext context) {

		if (this.timer == null) {
			this.timer = new Timer(true);
			this.timer.schedule(new TimerTask() {
				@Override
				public void run() {
					context.progress();
				}
			}, delay, interval);
		}

	}

	
	public void startHeartbeat(final Reporter reporter) {

		if (this.timer == null) {
			this.timer = new Timer(true);
			this.timer.schedule(new TimerTask() {
				@Override
				public void run() {
					reporter.progress();
				}
			}, delay, interval);
		}

	}
	
	public void stopHeatbeat() {
		
		if (this.timer != null) {
			this.timer.cancel();
			this.timer = null;
		}
		
	}

}
