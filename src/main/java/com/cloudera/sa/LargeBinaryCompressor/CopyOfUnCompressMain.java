package com.cloudera.sa.LargeBinaryCompressor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyOfUnCompressMain {

	public static void main(String[] args) throws IOException {
        if (args.length == 0) {
			System.out.println("---------");
			System.out.println("UnCompressMain <InputFolder> <outputFile>");
			System.out.println("---------");
			return;
		}
		
		Path inputFolder = new Path(args[0]);
		Path outputFile = new Path(args[1]);
		
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		if (!fs.exists(inputFolder) || !fs.isDirectory(inputFolder)) {
			System.out.println("!!!" + inputFolder + " is not a folder or doesn't exist.");
			return;
		}
		
		if (fs.exists(outputFile)) {
			System.out.println("!!!" + outputFile + " Already exist");
			return;
		}
		
		int i = 0;
		Path inputFile = new Path(inputFolder + "/part-m-" + StringUtils.leftPad(Integer.toString(i++), 5, '0'));
		byte[] inputBytes = new byte[1002];
		
		BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(outputFile));
		
		System.out.println("Starting---");
		
		while (fs.exists(inputFile)) {
			
			System.out.println("Reading:" + inputFile);	
			
			BufferedInputStream inputStream = new BufferedInputStream(new GZIPInputStream(fs.open(inputFile)));
			
			int len = 0;
			
			while((len = inputStream.read(inputBytes)) > 0) {
				
				if (len != 1002) {
					System.out.println("len:" + len);
					
				}
				
				outputStream.write(inputBytes,0, len );
			}
			
			inputStream.close();
			
			inputFile = new Path(inputFolder + "/part-m-" + StringUtils.leftPad(Integer.toString(i++), 5, '0'));
		}
		
		outputStream.close();
		System.out.println("Finished---");
		
	}
}
