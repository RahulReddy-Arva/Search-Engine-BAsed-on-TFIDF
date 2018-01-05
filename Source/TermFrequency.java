/**
Name: Rahul Reddy Arva
ID: 800955965
Title: TermFrequency
Description: This is a MapREduce program. Term frequency is defined as number of times a word is occurred in a specific document . We can use the below formula to calculate term frequency.
	TF = 1 + log10(number of times a word occurs in the document/ total number of words in document)
 **/
package rarva;
/**
 * @author cloudera
 **/
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.lang.Object;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; //import for using FileSPlit to get the name of the document
import java.lang.*;
import org.apache.hadoop.io.DoubleWritable;
//Main Class starts here
public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }
   //Run Method starts here
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfreq ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      job.waitForCompletion( true);
      return 0;
   } 
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > { // // This Map task is used to write 1 beside every occurrence of a word from the input file.
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*"); //This is used to ignore white space characters like tab, space
      public void map( LongWritable offset,  Text lineText,  Context context)// Map task starts here
        throws  IOException,  InterruptedException {
         String line  = lineText.toString(); // converts an entire line to a string.
         Text present_word  = new Text();
         for ( String word_details  : WORD_BOUNDARY .split(line)) { //splits the line into words when encountered by a white space character 'space'.
            if (word_details.isEmpty()) {
               continue;
            }
	    String filename = ((FileSplit) context.getInputSplit()).getPath().getName().toString(); // This is used to find the path & name of the document where the word is present.
	    word_details += "#####" + filename; //this is used to append the delimiter ##### And filename at the end of the each word.
	    present_word  = new Text(word_details); 
            context.write(present_word,one); //It is used to write a word to a file.
         }
      }
   }
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > { 
	   // This is the reducer task. It takes the input from Map task and then takes each unique occurrence of a word and then sums it up and then writes it to the file.
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int occurrences  = 0;
	 double word_frequency=0.0;
         for ( IntWritable count  : counts) {
        	 occurrences  += count.get(); // calculates the overall occurrences of a unique word
        	 word_frequency=1+Math.log10(occurrences); //this is the formula used to calculate word frequency of a specific word.
         }
         context.write(word,  new DoubleWritable(word_frequency)); // Writes all the words output file.
      }
   }
}