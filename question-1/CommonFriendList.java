
import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonFriendList {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text pairFriends = new Text(); // Output key - pair of friend
												// ids

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Set of user id pair for which the mutual list friend has to be
			// found
			Set<String> set = new HashSet(Arrays.asList("0,1", "20,28193", "1,29826", "6222,19272", "28041,28056"));

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
					// friends corresponding to user A if set contains the pair
					if (set.contains(pairFriends.toString()))
						context.write(pairFriends, new Text(userinfoLine[1]));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		// list of mutual friends output
		private Text mutual = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder str = new StringBuilder();
			Set<String> set = new HashSet<String>();

			// values contain list of friends for userA and second list of
			// friends for user B
			for (Text friend_list : values) {
				List<String> list = Arrays.asList(friend_list.toString().split(","));

				// iterating over the lists and finding mutual friends
				for (String friend : list) {

					if (set.contains(friend))
						str.append(friend + ','); // if friend already present in set then mutual so append to string							
					else
						set.add(friend);
				}
			}
			
			//if string not empty 
			if (str.lastIndexOf(",") > -1) {
				str.deleteCharAt(str.lastIndexOf(",")); // delete last ','
			}

			mutual.set(new Text(str.toString()));
			
			//output pair of friend and string of mutual friends
			context.write(key, mutual);
			
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "Q1CommonFriend");
		job.setJarByClass(CommonFriendList.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// HDFS path set for the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// HDFS path set for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
