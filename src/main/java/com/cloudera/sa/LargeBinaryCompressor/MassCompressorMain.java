package com.cloudera.sa.LargeBinaryCompressor;

import java.io.IOException;


import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.cloudera.sa.LargeBinaryCompressor.inputformat.FixedLengthInputFormat;

public class MassCompressorMain {

	public static String OUTPUT_FOLDER = "custom.output.folder";

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		if (args.length == 0) {
			System.out.println("---------");
			System.out.println("MassCompressor <inputFile> <outputFolder> ");
			System.out.println("---------");
			return;
		}


		// Get values from args
		String inputFile = args[0];
		String outputFolder = args[1];
        String compressionCodec = args[2];

        Configuration conf  = new Configuration();

        conf.set("compressionCodec", compressionCodec);

        Job job = new Job(conf);


        // Create job

		job.getConfiguration().set(OUTPUT_FOLDER, outputFolder);
        job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
        job.getConfiguration().set("mapred.task.timeout","0");
                

		job.setJarByClass(MassCompressorMain.class);
		job.setJobName("MassCompressor:" + inputFile + "->" + outputFolder);
		// Define input format and path
		job.setInputFormatClass(FixedLengthInputFormat.class);
		FixedLengthInputFormat.addInputPath(job, new Path(inputFile));
		FixedLengthInputFormat.setRecordLength(job, 1024);

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outputFolder + "/tmp"));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);

		// Define the key and value format
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		FileSystem hdfs = FileSystem.get(new Configuration());
		if (!hdfs.isFile(new Path(inputFile))
				|| !hdfs.exists(new Path(inputFile))) {
			System.out.println("!!!!");
			System.out.println(inputFile + " is not a file");
			System.out.println("!!!!");
			return;
		}

		// Exit
		job.waitForCompletion(true);

		hdfs.delete(new Path(outputFolder + "/tmp"), true);
	}

	public static class CustomMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		static char delimiter;
		public static String lineSeparator = System
				.getProperty("line.separator");

		int expectedNumberOfColumns = 0;

		//GZIPOutputStream outputStream = null;
        CompressorOutputStream outputStream = null;
        Text newKey = new Text();
		Text newValue = new Text();

		@Override
		public void setup(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String outputFolder = context.getConfiguration().get(OUTPUT_FOLDER);
            String compressionCodec = context.getConfiguration().get("compressionCodec");
            String extension = "";
            if (compressionCodec.equals("gzip")) {
            extension = ".gz";
            } else if (compressionCodec.equals("bzip2")){
              extension = ".bz2";
            }

            System.out.println("THe codec is : " + compressionCodec);

			FileSplit s = (FileSplit)context.getInputSplit();
			
			Path outputPath = new Path(outputFolder
					+ "/part-m-" 
					+ StringUtils.leftPad(Long.toString(s.getStart()), 20, "0")
					+ "-"
					+ StringUtils.leftPad(
							Integer.toString(context.getTaskAttemptID()
									.getTaskID().getId()), 5, '0')
                   + extension);

			try {
            if (compressionCodec.equals("gzip")) {
            outputStream = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, fs.create(outputPath));
            } else if (compressionCodec.equals("bzip2")) {
            outputStream = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, fs.create(outputPath));
		}
            }
            catch (CompressorException c1) {
            }
            }
		long lastLength = 0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			byte[] bytes = value.getBytes();
			int len = Integer.parseInt(key.toString());
			if (len != lastLength) {
				System.out.println("key different:" + lastLength + " " + len + " " + bytes.length);
				byte[] shortRecord = new byte[len];
				System.arraycopy(bytes, 0, shortRecord, 0, len);
				outputStream.write(shortRecord);
				lastLength = len;
			} else {
				outputStream.write(bytes);
			}
			
			
			
		}

		@Override
		public void cleanup(Context context) throws IOException {
			outputStream.close();
		}
	}

}
