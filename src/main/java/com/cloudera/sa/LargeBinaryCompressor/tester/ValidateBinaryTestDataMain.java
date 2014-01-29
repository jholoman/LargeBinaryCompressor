package com.cloudera.sa.LargeBinaryCompressor.tester;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ValidateBinaryTestDataMain {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("---------");
			System.out.println("ValidateCharTestData <filePath> ");
			System.out.println("---------");
			return;
		}
		
		String filePath = args[0];
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		FSDataInputStream input = fs.open(new Path(filePath));
		
		byte[] readBytes = new byte[998];
		
		byte goldValue = 0;
		
		int len = 0;
		
		long readCounter = 0;
		
		String lastString = "";
		
		
		
		
		while((len = input.read(readBytes)) > 0) {
			
			//System.out.print(len);
			
			for ( int i = 0; i < len; i++) {
				
				//System.out.println(goldValue + "," + readBytes[i]);
				
				if (goldValue != readBytes[i]) {
					System.err.println("Expected " + goldValue + " but got " + readBytes[i]);
				}
				goldValue++;
			}
			
			
			
			if (readCounter % 1000 == 0) {
				System.out.print(".");
			}
			if (readCounter % 100000 == 0) {
				System.out.println("|");
			}
			readCounter++;
		}
		
		input.close();
		System.out.println("Finished");
	}
}
