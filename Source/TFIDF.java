/*
 Name: Rahul Reddy Arva
 ID: 800955965
 Title: TFIDF
 Description: This is  MapReduce program. This program calculates the TFIDF Of the word. It calculates that using term frequency and inverse document frequency.
 		TF = 1 + log10(number of times a word occurs in the document/ total number of words in document)
 		IDF = log10(total number of documents/number of documents with term in it)
 		TFIDF = TF * IDF
 */

package rarva;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;   //import for using FileSPlit to get the name of the document
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;


public class TFIDF extends Configured implements Tool {
    
	private static final Logger LOG = Logger.getLogger(TFIDF.class);
    
    public static void main(String[] args) throws Exception {
    	int toolRunner = ToolRunner.run(new TermFrequency(),args);
    	//  The execution of TFIDF is done only if termFrequency is executed successfully.   	
    	if (toolRunner == 0){
    		int res = ToolRunner.run(new TFIDF(), args);
            System.exit(res);
    	} else{ 
    	}
    }
   
    //Run method starts here
    public int run(String[] args) throws Exception { // All the files are given here as input
        Configuration config = getConf();
        FileSystem fileSystem = FileSystem.get(config);
        final int numFiles = fileSystem.listStatus(new Path(args[0])).length;
        config.setInt("no_of_files", numFiles);
        Job job = Job.getInstance(getConf(), "tfidf"); // A job is created for the execution of TFIDF
        job.setJarByClass(this.getClass());
        // Input is taken from arg[1] and output is written into arg[2]
        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class); //set output type
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

 
    // Map function for TFIDF 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        //private Text word = new Text();
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String lineInput 		= lineText.toString();
            String finalValue 		= "";      
            String term 			= lineInput.split("#####")[0]; // Each line is split based on delimiter '#####'
            String file_tf 			= lineInput.split("#####")[1];
            String fileName 		= file_tf.split("\\t")[0];
            String tf 				= file_tf.split("\\t")[1];            
            finalValue 				= fileName + "=" + tf;
            context.write(new Text(term), new Text(finalValue)); // output is written
        }
    }

    // Reduce function for TFIDF
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {          
        @Override
        public void reduce(Text word, Iterable<Text> inputs, Context context)
                throws IOException, InterruptedException {
            double idf = 0.0;
            double tfidf = 0.0;
            String key = "";
            long numFiles = context.getConfiguration().getInt("no_of_files", 0);
            HashMap<String, Double> file_tfidf = new HashMap<String, Double>(); //we are using hashmap to store the key-value pairs
            ArrayList<String> arraylist = new ArrayList<String>(); // We are using arraylist to store the filenames. We are using a separate data structure which makes it easier to access.
            String fileName = null;
            for (Text file : inputs) {
                String file_value = file.toString();     
                key = word.toString() + "#####" + file_value.substring(0, file_value.indexOf("="));
                fileName = file_value.substring(0, file_value.indexOf("="));
                double tf = Double.parseDouble(file_value.substring(file_value.indexOf("=") + 1, file_value.length()));
                file_tfidf.put(key, tf);// store the key value pairs
                if (arraylist.contains(fileName)){
                	 // store the filename of unique words
                } else{
                	arraylist.add(fileName);
                }
           }
            int count = arraylist.size(); 
            idf = Math.log10(1 + (numFiles / count)); //calculate the inverse document frequency based on the formula
            for (String term : file_tfidf.keySet()) {
                tfidf = file_tfidf.get(term) * idf; // calculated TFIDF which is the product of term frequency and inverse document frequency.
                context.write(new Text(term), new DoubleWritable(tfidf)); //Each unique word and its corresponding TFIDF value is written into a file.
            }
        }
    }
}