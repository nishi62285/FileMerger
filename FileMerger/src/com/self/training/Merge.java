package com.self.training;
import java.io.*;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
public class Merge  {
     
public static void main(String[] args)
{
 
	try {
		Merge();
	} catch (URISyntaxException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

public static void Merge() throws URISyntaxException
{
try 
{
	PrintWriter metaWriter = new PrintWriter("/root/Desktop/Data/meta.txt");
	java.nio.file.Path metaPath = Paths.get("/root/Desktop/Data/meta.txt");
	String metaData = new String(Files.readAllBytes(metaPath));
	File dir = new File("/root/Desktop/Data/BatchData/");
	String[] fileNames = dir.list();
	if(metaData.length()>0)
	{
	  for (String fileName : fileNames)
	  {
		  if (metaData.contains(fileName))
		{
		  File f = new File("/root/Desktop/Data/"+fileName);
		  f.delete();
		}
		 else
		  {	 
			 metaWriter.println(Arrays.toString(fileNames));
			 metaWriter.close();
		  }
	 }
	}
	 else
	  {
		 metaWriter.println(Arrays.toString(fileNames));
		 metaWriter.close();
	  }
	Configuration conf = new Configuration();
	FileSystem fs =  FileSystem.get(conf);	
	SequenceFile.Writer writer = new SequenceFile.Writer
	(fs, conf, new Path("/root/Desktop/Data/test.seq"), Text.class, Text.class);
	
	for(String fileName : fileNames) {
		System.out.println("reading file :" + fileName);	
		java.nio.file.Path p = Paths.get("/root/Desktop/Data/BatchData/"+fileName);
		String content = new String(Files.readAllBytes(p));
		writer.append(new Text(fileName), new Text(content));
	}
	writer.close();
}
catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}	
}	
}