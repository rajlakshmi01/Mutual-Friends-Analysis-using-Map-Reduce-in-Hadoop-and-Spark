//Author Rajlakshmi Maurya
// Find the average number of mutual friends in the dataset and show the friends who have mutual friends above the
//average number of mutual friends.
import java.io.IOException;
import java.util.Arrays;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	
// Map extends with Mapper function which is used to represents long value text, data, output key & value that Mapper will produce
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// Splits the input line into two parts using a tab ("\t") delimiter.
            String[] line = value.toString().split("\t");
            if (line.length == 2) {
                // set friend 1 = first number
                String friend1 = line[0];
                List<String> values = Arrays.asList(line[1].split(","));
                // now iterate over list of the friends and compare the value of friend 1 & 2
                for (String friend2 : values) {
                    int f1 = Integer.parseInt(friend1);
                    int f2 = Integer.parseInt(friend2);
                    if (f1 < f2)
                        word.set(friend1 + "," + friend2);
                    else
                        word.set(friend2 + "," + friend1);
                    context.write(word, new Text(line[1]));
                }
            }
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            StringBuilder sbuilder = new StringBuilder();
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (map.containsKey(friend))
                        sbuilder.append(friend + ','); // append to string if friend already present
                    else
                        map.put(friend, 1);

                }
            }
            
            if (sbuilder.length() > 0) { // Check if the list is not empty // Print only those pairs which have mutual friends.
                sbuilder.deleteCharAt(sbuilder.lastIndexOf(","));
                result.set(new Text(sbuilder.toString()));
                context.write(key, result);
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "mutual friends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        // set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}