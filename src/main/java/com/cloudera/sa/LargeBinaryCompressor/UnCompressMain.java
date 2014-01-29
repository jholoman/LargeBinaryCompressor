package com.cloudera.sa.LargeBinaryCompressor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UnCompressMain {

	public static void main(String[] args) throws IOException {
        if (args.length == 0) {
			System.out.println("---------");
			System.out.println("UnCompressMain <InputFolder> <outputFile>");
			System.out.println("---------");
			return;
		}
		
		Path inputFolder = new Path(args[0]);
		Path outputFile = new Path(args[1]);
        String compressionCodec = args[2];
		
		
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
		
		FileStatus[] fileStatusArray = fs.listStatus(inputFolder);
		
		ArrayList<String> fileNameList = new ArrayList<String>();
		for (FileStatus fileStatus: fileStatusArray) {
			fileNameList.add(fileStatus.getPath().getName());
		}
		
		Collections.sort(fileNameList);
		
		BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(outputFile));
        BufferedInputStream inputStream = null;
		
		byte[] inputBytes = new byte[1002];
		
		for (String fileName: fileNameList) {
			if (fileName.startsWith("part-m")) {
				System.out.println("Reading:" + fileName);	
				
				//BufferedInputStream inputStream = new BufferedInputStream(new GZIPInputStream(fs.open(new Path(inputFolder + "/" + fileName))));

                try {
                    if (compressionCodec.equals("gzip")) {
                       inputStream = new BufferedInputStream(new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, fs.open(new Path(inputFolder + "/" + fileName))));
                    } else if (compressionCodec.equals("bzip2")) {
                        inputStream = new BufferedInputStream(new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, fs.open(new Path(inputFolder + "/" + fileName))));
                    }
                }
                catch (CompressorException c1) {
                }
                int len = 0;
				
				while((len = inputStream.read(inputBytes)) > -1) {
					
					if (len != 1002) {
						System.out.println("len:" + len);
						
					}
					
					outputStream.write(inputBytes,0, len );
				}
				
				inputStream.close();
			}
		}

		
		outputStream.close();
		System.out.println("Finished---");
		
	}
}
