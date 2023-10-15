
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
    public static class Map_country
            extends Mapper<LongWritable, Text, Text, Text>{
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {     	
        	String[] mydata = value.toString().replaceAll("\"", "").split(",");
        	Text capital = new Text(mydata[1]);
        	Text country = new Text("Country" +"	"+ mydata[0]);
        	context.write(capital, country);  
        }
    }
    
    public static class Map_avgTemp
    extends Mapper<LongWritable, Text, Text, Text>{
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    		String[] mydata = value.toString()
    							   .replaceAll("\"", "")
    							   .split(",");
    		if(!mydata[7].equals("AvgTemperature")) {
    			if(!mydata[7].equals("-99")) {
    				Text city = new Text(mydata[3]);
    				context.write(city, new Text("temperature"+"	"+ mydata[7]+"	"+mydata[1])); 
    			}     
    		}
    	}
    }
    
    public static class Reduce
            extends Reducer<Text,Text, Text, Text> { 

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country1 = "";
            String country2 = "";
        	float sum = 0; // initialize the sum for each keyword
            float num = 0;
            for (Text t : values) {
            	String[] parts = t.toString().replaceAll("", "").split("	");
            	if(parts[0].equals("Country")) {
            		country1 = parts[1];
            	}else if(parts[0].equals("temperature")) {
            		if(parts[2].equals("US")) {
            			country2 = "United States";
            			sum += Float.parseFloat(parts[1]);
                		num++;
            		}else {
            			country2 = parts[2];
                		sum += Float.parseFloat(parts[1]);
                		num++;            			
            		}
            		
            	}
            }
            String country11 = country1.replaceAll("\\p{Punct}", "")           // get rid of numbers...
     			   					   .replaceAll("[^a-zA-Z0-9 ]", "")
     			   					   .replaceAll("(?m)^[ \t]*\r?\n", "")
     			   					   .toLowerCase()
     			   					   .trim()
     			   					   .replaceAll("\\s+", " ");
            String country22 = country2.replaceAll("\\p{Punct}", "")           // get rid of numbers...
 					   				   .replaceAll("[^a-zA-Z0-9 ]", "")
 					   				   .replaceAll("(?m)^[ \t]*\r?\n", "")
 					   				   .toLowerCase()
 					   				   .trim()
 					   				   .replaceAll("\\s+", " ");
            if(country11.equals(country22)) {
            	float avgtemp = sum / num;
            	context.write(new Text(country2), new Text(key + " " + Float.toString(avgtemp)));
            }
            
        }
        
    }
    
      
        
 

    
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        
        //job.setMapperClass(Maps.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setSortComparatorClass(SortFloatComparator.class);
        
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
    
     // set output value type
        job.setOutputValueClass(FloatWritable.class);
        // set output key type
        job.setOutputKeyClass(Text.class);
        
       
        //set the HDFS path of the input data
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map_country.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Map_avgTemp.class);
        // set the HDFS path for the output
        Path outputPath = new Path(otherArgs[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}