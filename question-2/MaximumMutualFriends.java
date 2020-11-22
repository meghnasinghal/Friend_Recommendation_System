import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


public class MaximumMutualFriends {
    public static class FriendsListMap extends Mapper<LongWritable, Text, Text, Text> {
    	
        private Text pairFriends = new Text(); // Output key - pair of friend ids
	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// userID and list of friends on splitting
			String[] userinfoLine = value.toString().split("\t");

			if (userinfoLine.length == 2) {
				// set friend id 1 = userID
				String userA = userinfoLine[0];

				// get list of friends by splitting on ','
				List<String> values = Arrays.asList(userinfoLine[1].split(","));
				int id1 = Integer.parseInt(userA);

				// iterating over list of friends of user A
				for (String userB : values) {
					int id2 = Integer.parseInt(userB);
					// generating pair sorted based on id to give out as output
					// key
					if (id1 < id2)
						pairFriends.set(userA + "," + userB);
					else
						pairFriends.set(userB + "," + userA);

					// output key as sorted pair of friend id and list of
					// friends corresponding to user A
					context.write(pairFriends, new Text(userinfoLine[1]));
				}
			}
		
        }
    }

   static int max_count = Integer.MIN_VALUE;
    public static class FriendsListReduce extends Reducer<Text, Text, Text, IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	int count_mutual = 0;
			Set<String> set = new HashSet<String>();

			// values contain list of friends for userA and second list of
			// friends for user B
			for (Text friend_list : values) {
				List<String> list = Arrays.asList(friend_list.toString().split(","));

				// iterating over the lists and finding mutual friends
				for (String friend : list) {

					if (set.contains(friend))
						count_mutual++; // if friend already present in set then mutual 						
					else
						set.add(friend);
				}
			}
			
			max_count = Math.max(max_count, count_mutual); // finding the maximum number of mutual friends
				
			//output pair of friend and number of mutual friends
			context.write(key, new IntWritable(count_mutual));
			
		}
    }

    public static class MaxMutualFriendsMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable constKey = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            {
                context.write(constKey, value); // giving out 1 as key to aggregate all the pairs at a single reduce call and value as content of the previous reduce output
            }
        }
    }

    public static class MaxMutualFriendsReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    		//iterating over all lines of content
            for (Text l : values) {
            	//split to get pair and the count of mutual friends for the pair
                String[] split = l.toString().split("\t");
                
                if (split.length == 2) {
                	//check if the count is max
                   if(Integer.parseInt(split[1]) ==  max_count)
                   {
                	   // output the pairs with max mutual friends
                	   context.write(new Text(split[0]), new IntWritable(max_count));  
                	   
                   }
                }
            }

	
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

      
        Job firstjob = new Job(conf, "Q2MaxMutualFriend");
        firstjob.setJarByClass(MaximumMutualFriends.class);
        firstjob.setMapperClass(FriendsListMap.class);
        firstjob.setReducerClass(FriendsListReduce.class);
 
        firstjob.setOutputKeyClass(Text.class);
        firstjob.setOutputValueClass(Text.class);
        
        // HDFS path set of the input data
        FileInputFormat.addInputPath(firstjob, new Path(otherArgs[0]));
        // HDFS path set for the output
        FileOutputFormat.setOutputPath(firstjob, new Path(otherArgs[1]));

        if (firstjob.waitForCompletion(true)) {
            // second job send to output of first job 
            Configuration secondJobConf = new Configuration();
            Job secondjob = Job.getInstance(secondJobConf);

            secondjob.setJarByClass(MaximumMutualFriends.class);
            secondjob.setMapperClass(MaxMutualFriendsMap.class);
            secondjob.setReducerClass(MaxMutualFriendsReduce.class);
            secondjob.setInputFormatClass(TextInputFormat.class);

            secondjob.setMapOutputKeyClass(IntWritable.class);
            secondjob.setMapOutputValueClass(Text.class);

            secondjob.setOutputKeyClass(Text.class);
            secondjob.setOutputValueClass(IntWritable.class);

            // input of the second job set as output of the first
            FileInputFormat.addInputPath(secondjob, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(secondjob, new Path(otherArgs[2]));

            System.exit(secondjob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
