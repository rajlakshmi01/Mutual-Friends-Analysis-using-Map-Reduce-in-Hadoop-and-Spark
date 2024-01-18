//Author: Rajlakshmi Maurya
//Description: Map-Reduce logic to count Average Mutual Friends and display the pairs whose count is above average.
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java .util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriendAvg{
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length == 2) {
                // setting friend 1 the very first number
                String friend1 = line[0];
                List<String> values = Arrays.asList(line[1].split(","));
                // iterating over list of the friends and comparing the value of friend 1 & 2
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
        HashMap<Text, Integer> mutualFriendsCount = new HashMap<Text,Integer>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            int count=0;
            StringBuilder sbuilder = new StringBuilder();
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (map.containsKey(friend)) {
                        sbuilder.append(friend + ','); // append to string if friend already present
                        count+=1; 
                        }  // counting total number of mutual friends
                    else
                        map.put(friend, 1);

                }
            }
            
            if (sbuilder.length() > 0) { // Check if the list is not empty .
                sbuilder.deleteCharAt(sbuilder.lastIndexOf(","));
                result.set(new Text(sbuilder.toString()));// Print only those pairs which have mutual friends
                
            }
            mutualFriendsCount.put(new Text(key),count);//set the count of each mutual friend pair
                   }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int totalPairs = 0;
            int totalMutualFriends = 0;
            
            for (Entry<Text, Integer> entry : mutualFriendsCount.entrySet()) {
                if (entry.getValue() > 0) {
                    totalPairs++; //Calculating totalPairs which have mutual friends, ignoring the Pairs with 0 mutual friends
                    totalMutualFriends += entry.getValue(); //Counting total Mutual Friends
                }
            }
            
            if (totalPairs > 0) {
                double averageMutualFriends = (double) totalMutualFriends / totalPairs; // Calculating average

                context.write(new Text("Average Mutual Friends"), new Text(String.format("%.2f", averageMutualFriends)));

                // Display the pairs with mutual friends above the average
                for (Entry<Text, Integer> entry : mutualFriendsCount.entrySet()) {
                    if (entry.getValue() > averageMutualFriends) {
                        context.write(entry.getKey(), new Text(entry.getValue().toString())); //displays the pairs and their mutualFriend count > average
                    }
                }
            }
        }

        
           }
 

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "mutual friends");
        job.setJarByClass(MutualFriendAvg.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        // setting the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // setting the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}