package com.cloudera.sa.LargeBinaryCompressor.tester;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenCharTestDataMain {

	public static void main(String[] args) throws IOException {
		
		if (args.length == 0) {
			
			System.out.println("---------");
			System.out.println("GenCharTestData <filePath> <numberOfKb>");
			System.out.println("---------");
			return;
		}
		
		String filePath = args[0];
		long numberOfKb = Long.parseLong(args[1]);
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		BufferedWriter output = new BufferedWriter(new  OutputStreamWriter(fs.create(new Path(filePath))));
		
		System.out.println("Starting ------");
		
		
		for (long i = 0; i < numberOfKb; i++) {
			
			String currentNumber = StringUtils.leftPad(Long.toString(i), 999);
			
			output.write(currentNumber);
			
			if (i % 1000 == 0) {
				System.out.print(".");
			}
			if (i % 100000 == 0) {
				System.out.println("|");
			}
		}
		
		output.close();
		System.out.println("Finished ------");
	}
}
