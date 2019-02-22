package com.narata.tools.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


/**
 * @author narata
 * @since 2019/02/22
 */
public class JobBuilder {
	/**
	 * 快捷 mr 中 input，output 路径输入
	 * @param tool Driver类
	 * @param conf 配置文件
	 * @param args 命令行输入的参数
	 * @return job
	 * @throws Exception
	 */
	public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws Exception {
		if (args.length < 2) {
			printUsage(tool);
			return null;
		}
		Job job = new Job(conf);
		job.setJarByClass(tool.getClass());
		for (int i = 0; i < args.length-1; i++) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		return job;
	}

	private static void printUsage(Tool tool) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), "<input1> ... <output>");
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
