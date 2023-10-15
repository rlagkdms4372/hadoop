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
	public static <K,V extends Comparable<V> > Map<K, V> valueSort(final Map<K, V> map){
    	Comparator<K> valueComparator = new Comparator<K>() {
    		public int compare (K k1, K k2) {
    			int comp = map.get(k1).compareTo(map.get(k2));
    			if(comp == 0)
    				return 1;
    			else
    				return comp;
    		}
    	};
    	Map<K, V> sorted = new TreeMap<K, V>(valueComparator);
    	sorted.putAll(map);
    	return sorted;
    }

    public static class Maps
            extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private TreeMap<Integer, Text> topWords = new TreeMap<Integer, Text>();
        private Text word = new Text(); // type of output key
        private final static int TOP_N = 10;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String[] mydata = value.toString()
                    			   .replaceAll("\\p{Punct}", "")           // get rid of numbers...
                    			   .replaceAll("[^a-zA-Z0-9 ]", "")
                    			   .replaceAll("(?m)^[ \t]*\r?\n", "")
                    			   .toLowerCase()
                    			   .trim()
                    			   .replaceAll("\\s+", " ")
                                   .split(" ");
        	List<String> list = new ArrayList<String>();

            for(String s : mydata) {
               if(s != null && s.length() > 0) {
                  list.add(s);
               }
            }
            
            mydata = list.toArray(new String[list.size()]);
            for (String data : mydata) {
                word.set(data); // set word as each input keyword
                context.write(word, one); // create a pair <keyword, 1>
            }
        
        }
      
    }

    public static class Reduce
            extends Reducer<Text,IntWritable, Text, IntWritable> {
    	private TreeMap<Integer, Text> topWords = new TreeMap<Integer, Text>();
    	
    	private final static int TOP_N = 10;
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            for (IntWritable val : values) {
                sum += val.get();
            }
            topWords.put(sum, new Text(key.toString()));
            
            if (topWords.size() > TOP_N) {
                topWords.remove(topWords.firstKey());
            }
        }
           
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	
        	for(Map.Entry<Integer, Text> entry:topWords.descendingMap().entrySet() ) {
        		int count = entry.getKey();
            	Text name = entry.getValue();
            	context.write( new Text(name), new IntWritable(count));
            	}
        	
                
       
            }
        	
        
        
        
    }
    
//    public class SortFloatComparator extends WritableComparator {
//
//    	//Constructor.
//    	 
//    	protected SortFloatComparator() {
//    		super(FloatWritable.class, true);
//    	}
//    	
//    	@SuppressWarnings("rawtypes")
//
//    	@Override
//    	public int compare(WritableComparable w1, WritableComparable w2) {
//    		FloatWritable k1 = (FloatWritable)w1;
//    		FloatWritable k2 = (FloatWritable)w2;
//    		
//    		return -1 * k1.compareTo(k2);
//    	}
//    }
    
    
        
        
 

    
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
        job.setMapOutputValueClass(IntWritable.class);

        //job.setSortComparatorClass(SortFloatComparator.class);
        
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
    
     // set output value type
        job.setOutputValueClass(IntWritable.class);
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