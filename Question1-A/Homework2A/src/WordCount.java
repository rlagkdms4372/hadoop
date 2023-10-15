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
import org.apache.commons.lang.StringUtils;

public class WordCount {
	
    public static class Maps
            extends Mapper<LongWritable, Text, Text, FloatWritable>{
        private final static FloatWritable one = new FloatWritable(1);
        private Text word = new Text(); 
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	String[] mydata = line.split(",");
        	if(!mydata[7].equals("AvgTemperature")) {
        		if(!mydata[7].equals("-99")) {
        			Text keyy = new Text(mydata[0].trim());
        			FloatWritable temp = new FloatWritable(Float.parseFloat(mydata[7]));
        			context.write(keyy, temp); // create a pair <keyword, 1>  
        		}
        	}
        }
      
    }

    public static class Reduce
            extends Reducer<Text,FloatWritable, Text, FloatWritable> {
    	
        

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0; // initialize the sum for each keyword
            float num = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                num += 1;
            }
            float avgtemp = sum / num;
            context.write(key, new FloatWritable(avgtemp)); // create a pair <keyword, number of occurences>
        }
        
        
    }
    
      
        
 

    
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        
        job.setMapperClass(Maps.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        //job.setSortComparatorClass(SortFloatComparator.class);
        
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
    
     // set output value type
        job.setOutputValueClass(FloatWritable.class);
        // set output key type
        job.setOutputKeyClass(Text.class);
        
       
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
