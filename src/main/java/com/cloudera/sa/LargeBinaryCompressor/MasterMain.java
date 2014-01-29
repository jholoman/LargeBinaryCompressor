package com.cloudera.sa.LargeBinaryCompressor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import com.cloudera.sa.LargeBinaryCompressor.tester.CheckJob;
import com.cloudera.sa.LargeBinaryCompressor.tester.GenBinaryTestDataMain;
import com.cloudera.sa.LargeBinaryCompressor.tester.GenCharTestDataMain;
import com.cloudera.sa.LargeBinaryCompressor.tester.ValidateBinaryTestDataMain;
import com.cloudera.sa.LargeBinaryCompressor.tester.ValidateCharTestDataMain;


public class MasterMain 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	
    	System.out.println("Version 0.2");
    	
    	int start = 100;
    	int recordByteLength = 10;
    	System.out.println(start - (start % recordByteLength) + recordByteLength);
    	
    	if (args.length == 0) {
			System.out.println("---------");
			System.out.println("MasterMain <cmd> ");
			System.out.println("");
			System.out.println("GenBinaryTestData");
			System.out.println("GenCharTestData");
			System.out.println("ValidateBinaryTestData");
			System.out.println("ValidateCharTestData");
			System.out.println("MassCompress");
			System.out.println("UnCompress");
			System.out.println("Check");
			System.out.println("---------");
			return;
		}
        
        String cmd = args[0];
        
        String[] subArgs = new String[args.length -1];
        System.arraycopy(args, 1, subArgs, 0, subArgs.length);
        
        if (cmd.equals("GenBinaryTestData")) {
        	GenBinaryTestDataMain.main(subArgs);
        } else if (cmd.equals("GenCharTestData")) {
        	GenCharTestDataMain.main(subArgs);
        } else if (cmd.equals("ValidateCharTestData")) {
        	ValidateCharTestDataMain.main(subArgs);
        } else if (cmd.equals("ValidateBinaryTestData")) {
        	ValidateBinaryTestDataMain.main(subArgs);
        } else if (cmd.equals("MassCompress")) {
        	MassCompressorMain.main(subArgs);
        } else if (cmd.equals("UnCompress")) {
        	UnCompressMain.main(subArgs);
        } else if (cmd.equals("Check")) {
        	CheckJob.main(subArgs);
        } else if (cmd.equals("simpleBinaryTest")) {
        	GenBinaryTestDataMain.main(new String[]{"gen.txt", "10"});
        	ValidateBinaryTestDataMain.main(new String[]{"gen.txt"});
        } else {
        	System.out.println("Unknown cmd:" + args[0]);
        }
        
    }
    
   
}
