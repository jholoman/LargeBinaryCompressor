package com.cloudera.sa.LargeBinaryCompressor.tester;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenBinaryTestDataMain {

	public static void main(String[] args) throws IOException {
		
		if (args.length == 0) {
			
			System.out.println("---------");
			System.out.println("GenBinaryTestData <filePath> <numberOfKb>");
			System.out.println("---------");
			return;
		}
		
		String filePath = args[0];
		long numberOfKb = Long.parseLong(args[1]);
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		FSDataOutputStream output = fs.create(new Path(filePath));
		
		System.out.println("Starting ------");
		
		byte indexCounter = 0;
		
		for (long i = 0; i < numberOfKb; i++) {
			
			byte[] b = new byte[1001];
			for (int k = 0; k < 1001; k++) {
				b[k] = indexCounter++;
			}
			
			output.write(b);
			
			
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
