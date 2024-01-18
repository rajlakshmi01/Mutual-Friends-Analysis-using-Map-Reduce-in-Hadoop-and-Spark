// Name: Rajlakshmi Maurya
// Second part:  Find two friends who have highest number of mutual friends. Also show the mutual friends between two peoples
//who’s first letter contains ‘1’ or ‘5’. (For testing purposes, in this example, you can use ‘a’ or ‘A’)
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendH1 {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      if (line.length == 2) {
        String friend1 = line[0];
        List<String> values = Arrays.asList(line[1].split(","));
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
   // private String highestFriendsPair = "";
    private int highestMutualFriendsCount = Integer.MIN_VALUE;
    HashMap<Text, Integer> mutualFriendsCount = new HashMap<Text,Integer>();
    HashMap<Text, String> mutualFriendsMap = new HashMap<Text,String>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        
        StringBuilder sbuilder = new StringBuilder();
        int count=0; // to store number of mutual friends
        for (Text friends : values) {
            List<String> temp = Arrays.asList(friends.toString().split(","));
            for (String friend : temp) {
                if (map.containsKey(friend)) {
                    sbuilder.append(friend + ',');
                	count+=1;}
                else
                    map.put(friend, 1);
            }
        }

        if (sbuilder.length() > 0) {
            sbuilder.deleteCharAt(sbuilder.lastIndexOf(","));
            result.set(new Text(sbuilder.toString()));
            mutualFriendsMap.put(new Text(key),result.toString());
        }
  
        mutualFriendsCount.put(new Text(key),count); //setting the mutual friend count for each pair with key as pair and count as count value
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Iterating through mutual friends and find friends with highestmutualfriend count
    	for (Entry<Text, Integer> entry : mutualFriendsCount.entrySet()) {
            if (entry.getValue() > highestMutualFriendsCount) {
                highestMutualFriendsCount = entry.getValue();  //max count value
                //context.write(new Text("Highest Mutual Friend count is: "), new Text(highestMutualFriendsCount.));
            }
        }
       for (Entry<Text,Integer>entry: mutualFriendsCount.entrySet()) {
    	   
        // Output friends pair with highest mutual friends count
    	  if (entry.getValue() == highestMutualFriendsCount) {
        context.write(new Text("Friends with Highest Mutual Friends Count:"), new Text(entry.getKey().toString()));
        context.write(new Text("Mutual Friends Count:"), new Text(Integer.toString(entry.getValue())));}}

        // Output mutual friends for pairs whose names start with '1' or '5'
        for (Entry<Text, String> entry : mutualFriendsMap.entrySet()) {
            String firstFriend = entry.getKey().toString().split(",")[0];
            String secondfriend=entry.getKey().toString().split(",")[1];
            if ((firstFriend.startsWith("1") || firstFriend.startsWith("5")) & (secondfriend.startsWith("1") || secondfriend.startsWith("5"))){
                context.write(new Text(entry.getKey().toString()), new Text(entry.getValue()));
            }
        }
    }
}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // First MapReduce job to find mutual friends
    Job job1 = new Job(conf, "mutual friends");
    job1.setJarByClass(MutualFriendH1.class);
    job1.setMapperClass(Map.class);
    job1.setReducerClass(Reduce.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
  }


