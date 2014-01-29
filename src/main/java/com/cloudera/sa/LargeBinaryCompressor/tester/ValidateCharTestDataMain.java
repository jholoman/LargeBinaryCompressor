package com.cloudera.sa.LargeBinaryCompressor.tester;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ValidateCharTestDataMain {
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
		BufferedReader read = new BufferedReader(new InputStreamReader(input));
		
		char[] readChar = new char[999];
		
		long goldValue = 0;
		
		int i = 0;
		
		int len = 0;
		
		String lastString = "";
		while((len = read.read(readChar)) > 0) {
			
			//System.out.print(len);
			
			
			String strValue = new String(readChar).trim();
			//System.out.println(" [" + strValue + "]" + strValue.equals("10"));
			try {
			long value = Long.parseLong(strValue);
				if (value != goldValue) {
					
					throw new RuntimeException("value:" + value + " != " + goldValue);
				}
			} catch (Exception e) {
				System.out.println("Fail");
				System.out.println("len:" + len);
				System.out.println("lastString:[" + lastString + "]");
				System.out.println("strValue:[" + strValue + "]");
				System.out.println("Expected:" + goldValue);
				
				e.printStackTrace();
				return;
			}
			
			goldValue++;
			
			if (i % 1000 == 0) {
				System.out.print(".");
			}
			if (i % 100000 == 0) {
				System.out.println("|");
			}
			i++;
			lastString = strValue;
		}
		
		read.close();
		System.out.println("Finished");
	}
}
