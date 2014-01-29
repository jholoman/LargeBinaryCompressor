package com.cloudera.sa.LargeBinaryCompressor.tester;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckJob {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("---------");
			System.out.println("Check <filePath> <filePath>");
			System.out.println("---------");
			return;
		}

		String file1Path = args[0];
		String file2Path = args[1];

		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);

		System.out.println("File1 Size: " + fs.getFileStatus(new Path(file1Path)).getLen());
		System.out.println("File2 Size: " + fs.getFileStatus(new Path(file2Path)).getLen());
		
		
		BufferedInputStream input1 = new BufferedInputStream(fs.open(new Path(file1Path)));
		BufferedInputStream input2 = new BufferedInputStream(fs.open(new Path(file2Path)));
		
		byte[] readBytes1 = new byte[1];
		byte[] readBytes2 = new byte[1];

		long checkSumTotal1 = 0;
		long checkSumTotal2 = 0;

		int len = 0;
		int len2 = 0;
		
		long unmatches = 0;
		
		long readCounter = 0;
		
		while ((len = input1.read(readBytes1)) > 0) {

			len2 = input2.read(readBytes2);
			
			if (len != len2) {
				throw new RuntimeException("Lens don't match:" + len + " " + len2);
			}
			
			// System.out.print(len);

			for (int i = 0; i < len; i++) {
				
				checkSumTotal1 += readBytes1[i]; 
				checkSumTotal2 += readBytes2[i];
				
				if (readBytes1[i] != readBytes2[i]) {
					unmatches++;
				}
				
			}

			if (readCounter % 1000000 == 0) {
				System.out.print(".");
			}
			if (readCounter % 100000000 == 0) {
				System.out.println("|");
			}
			readCounter++;
		}
		
		System.out.println("");
		System.out.println("Finished");
		System.out.println(" - File 1 CheckSum:" + checkSumTotal1);
		System.out.println(" - File 2 CheckSum:" + checkSumTotal2);
		System.out.println(" - Unmatching bytes:" + unmatches);
	}
}
