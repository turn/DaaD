/**
 * Copyright (C) 2015 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */
package com.turn.datamine.utils.mapreduce;

import static java.lang.Thread.sleep;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * The class defines a map-only job, whose map task runs the specified application. 
 * 
 * @author yqi
 */
public class RunAppAsMapTask extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(RunAppAsMapTask.class);
	private static final Random rand = new Random();
	private static final String ARGUMENT_DELIMITER = ";;;";
	
	private static final String APPLICATION_CLASS_PATH = "APPLICATION_CLASS_PATH";
	private static final String APPLICATION_CLASS_NAME = "APPLICATION_CLASS_NAME";
	private static final String APPLICATION_ARGUMENTS_NUM = "APPLICATION_ARGUMENTS_NUM";
	private static final String APPLICATION_ARGUMENTS = "APPLICATION_ARGUMENTS";

	/**
	 * The map task runs the application 
	 * @author yqi
	 * @date Oct 26, 2015
	 */
	static class ApplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

		private String[] args = new String[0];
		private Class<Tool> appClazz = null;
		private HadoopHeartBeat2 heartBeat = null;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			//1. get the class to run
			String appClazzPath = conf.get(APPLICATION_CLASS_PATH);
			String appClazzName = conf.get(APPLICATION_CLASS_NAME);
			Path[] cachedFiles = context.getLocalCacheFiles();
			List<URL> urlList = Lists.newArrayList();
			
			if (cachedFiles != null && cachedFiles.length > 0) {
				LocalFileSystem fs = FileSystem.getLocal(conf);
				for (int i=0; i<cachedFiles.length; ++i) {
					File curFile = fs.pathToFile(cachedFiles[i]);
					if (curFile.exists()) {
						urlList.add(curFile.toURI().toURL());
					} else {
						logger.error(String.format("File cannot be found - %s ", curFile));
					}
				}
				logger.info(String.format("Cached Files: %s", urlList));
			}
			
			if (!urlList.isEmpty()) {
				URLClassLoader child = new URLClassLoader (urlList.toArray(new URL[0]));
				try {
					appClazz = (Class<Tool>) Class.forName (appClazzName, true, child);
				} catch (ClassNotFoundException e) {
					logger.error(e.getMessage());
					throw new RuntimeException("Failed to load the class - " + appClazzName);
				}
			} else {
				String msg = String.format("No libs under %s can be found for %s", 
						appClazzPath, appClazzName);
				logger.error(msg);
				throw new RuntimeException(msg);
			}
			
			//2. get the application arguments
			int argumentNum = conf.getInt(APPLICATION_ARGUMENTS_NUM, 0);
			if (argumentNum > 0) {
				args = conf.get(APPLICATION_ARGUMENTS).split(ARGUMENT_DELIMITER);
				logger.info(String.format("Application arguments: %s ", Arrays.toString(args)));
			}
			
			super.setup(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//1. initiate the intermediate variables
			if (heartBeat == null) {
				heartBeat = new HadoopHeartBeat2();
				heartBeat.startHeartbeat(context);
			}
			
			//2. run the application 
			final String errMsg = String.format("Failed to run %s ", appClazz);
			try {
				logger.info(String.format("Run %s with argments %s", appClazz, Arrays.toString(args)));
				ToolRunner.run(context.getConfiguration(), 
						appClazz.newInstance(), args);
			} catch (InstantiationException e) {
				throw new RuntimeException(errMsg, e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(errMsg, e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(errMsg, e);
			} catch (Exception e) {
				throw new RuntimeException(errMsg, e);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (heartBeat != null) {
				heartBeat.stopHeatbeat();
			}
		}
	}
	
	public int run(final String[] args) {

		if (args.length < 2) {
			StringBuilder sb = new StringBuilder();
			sb.append("Please enter at least the following parameters in order delimited by space: \n");
			sb.append("1. the HDFS path where the lib is stored (e.g., /user/lib/)\n");
			sb.append("2. the full name of the class for the application (e.g., com.turn.datamine.sanitychecking.WorkExecution)\n");
			sb.append("3. the configuration for the application (e.g., GENERIC_OPTIONS - https://hadoop.apache.org/docs/r1.0.4/commands_manual.html) \n");
			sb.append("4. the arguments of the application (e.g., -i query1.xml) \n");
			System.err.println(sb.toString());
			logger.error(sb.toString());
			return 0;
		}
		
		//0. collect the arguments
		String classPath = args[0];
		String className = args[1];
		
		try {
			Configuration conf = super.getConf();
			if (conf == null) {
				conf = new Configuration();
			}

			//1. prepare a fake input file
			Path dummyFilePath = new Path(String.format("/tmp/dummyFile4applicationAsMapTask/%s__%s%s",
					className.replace('.', '_'), 
					System.currentTimeMillis(),
					rand.nextLong() ));
			FileSystem dfs = FileSystem.get(conf);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(dfs.create(dummyFilePath,true)));
			br.write("this is a dummy line!");
			br.close();

			//2. set the configuration
			//2.1 append the class path
			Path libPath = new Path(classPath);
			if (dfs.exists(libPath)) {
				FileStatus status = dfs.getFileStatus(libPath);
				if (status.isDirectory()) {
					List<Path> jarPathList = getFilePathList(dfs, libPath);
					for (Path cur : jarPathList) {
						DistributedCache.addFileToClassPath(cur, conf);
					}
				}
			}
			
			//2.2 the application class and its parameters
			conf.set(APPLICATION_CLASS_PATH, classPath);
			conf.set(APPLICATION_CLASS_NAME, className);
			conf.setInt(APPLICATION_ARGUMENTS_NUM, args.length - 2);
			if (args.length > 2) {
				StringBuilder sb = new StringBuilder();
				for (int i=2; i<args.length; ++i) {
					sb.append(args[i]).append(ARGUMENT_DELIMITER);
				}
				conf.set(APPLICATION_ARGUMENTS, sb.toString());
			}

			//2.3 others
			conf.setBoolean("mapred.map.tasks.speculative.execution", false);
			int maxMapAttempts = 1;
			conf.setInt("mapreduce.map.maxattempts", maxMapAttempts); 
			conf.setInt("mapred.map.max.attempts", maxMapAttempts); // old version
			
			//3. run the job
			Job job = new Job(conf);
			job.setJarByClass(RunAppAsMapTask.class);
			job.setJobName(String.format("Running %s as a map task", className));
			job.setMapperClass(ApplicationMapper.class);
			job.setNumReduceTasks(0);
			job.setInputFormatClass(TextInputFormat.class);			
			job.setUserClassesTakesPrecedence(true);
			
			TextInputFormat.addInputPath(job, dummyFilePath);
			Path outputDir = new Path(String.format("/tmp/dummyFile4applicationAsMapTask/%s__%s%s_output",
					className.replace('.', '_'), 
					System.currentTimeMillis(),
					rand.nextLong() ));
			TextOutputFormat.setOutputPath(job, outputDir);

			job.submit();
			while (!job.isComplete()) {
				logger.debug("waiting for job " + job.getJobName());
				sleep(100);
			}
			logger.info("status for job " + job.getJobName() + ": " + 
					(job.isSuccessful() ? "SUCCESS" : "FAILURE"));


			//4. clean up
			if (!job.isSuccessful()) {
				throw new RuntimeException("job failed " + job.getJobName());
			} else {
				dfs.delete(dummyFilePath, true);
				dfs.delete(outputDir, true);
			}
					
			return job.isSuccessful() ? 1 : 0;

		} catch (ClassNotFoundException e) {
			logger.error("Failed to run the applciation of " + className, e);
		} catch (InterruptedException e) {
			logger.error("Failed to run the applciation of " + className, e);
		} catch (IOException e) {
			logger.error("Failed to run the applciation of " + className, e);
		}

		return 0;
	}

	/**
	 * The main function as the entry point of the applications
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		int res = new RunAppAsMapTask().run(args);
		System.exit(res);
	}
	
	/**
	 * Get a list of all files under the input directory in the concerned file system. 
	 * 
	 * @param fs the input file system
	 * @param dirPath the path where the directory is
	 * @return a list of all files under the input directory in the concerned file system.
	 * @throws IOException thrown if the input path could be identified.
	 */
	public static final List<Path> getFilePathList(FileSystem fs, Path dirPath) throws IOException {
		List<Path> ret = new ArrayList<Path>();
		
		if (fs.exists(dirPath)) {
			FileStatus status = fs.getFileStatus(dirPath);
			if (status.isDirectory()) {
				FileStatus[] statusList = fs.listStatus(dirPath);
				for (FileStatus cur : statusList) {
					if (cur.isDirectory()) {
						ret.addAll(getFilePathList(fs, cur.getPath()));
					} else {
						ret.add(cur.getPath());
					}
				}
			} else {
				ret.add(dirPath);
			}
		}
		return ret;
	}
	
}
