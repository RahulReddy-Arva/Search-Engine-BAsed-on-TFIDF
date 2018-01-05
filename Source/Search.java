/**
 * Name: Rahul Reddy Arva
ID: 800955965
Title: Search 
Description: This is a MapReduce task used to implement a simple batch mode search engine. It takes a query as input and lists all the documents which best matches the query.
 */
package rarva;
/**
 * @author cloudera
 *
 */
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

//Main function
public class Search extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Search.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception { // creates job chaining with different map and reduce tasks
        
        final String key_word = args[2]; // pass the query as the 3rd argument
        getConf().set("key_word", key_word);
        Job job = Job.getInstance(getConf(), "search");
        job.setJarByClass(this.getClass());      	
        FileInputFormat.addInputPaths(job, args[0]); // get the input from argument 1 and write the data to 2nd argument.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);;
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> { // This is the Map function
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            String search = context.getConfiguration().get("key_word");
            
            search = search.toLowerCase();  //this converts the given search word to lowercase.
            String[] words = search.split(" "); // this splits the words based on spaces.
            String term = line.split("#####")[0]; // this line splits the line based on the delimiter'######'
            String file_tfidf = line.split("#####")[1];
            String file_name = file_tfidf.split("\\t")[0];
            String tfidf = file_tfidf.split("\\t")[1];

            for (String w : words) {
                if (w.equals(term)) {
                    context.write(new Text(file_name), new Text(tfidf)); // this is a mapper output which is the input to the reducer task.
                }
            }
        }
    }
    // This is the reduce task
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {           
        @Override
        public void reduce(Text word, Iterable<Text> list, Context context)
                throws IOException, InterruptedException {
            double final_score = 0.0;
		for (Text a : list) {
			final_score = final_score + Double.parseDouble(a.toString()); // This is the final TFIDF value.
            }
            context.write(new Text(word), new DoubleWritable(final_score));  //This writes the output to a file.
        }
    }
}