
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UserInfo {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		static HashMap<Integer, String> map = new HashMap<Integer, String>();

		// Reading the userdata.txt file and creating a map to access later
		protected void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status_arr = fs.listStatus(new Path(config.get("USERINFO")));
			for (FileStatus file_status : status_arr) {
				BufferedReader buffer = new BufferedReader(new InputStreamReader(fs.open(file_status.getPath())));
				String ln = buffer.readLine();
				while (ln != null) {
					String[] split = ln.split(",");
					if (split.length == 10) {
						// adding to map the friend id as key and the name :
						// date of birth as value
						map.put(Integer.parseInt(split[0]), split[1] + ":" + split[9]);
					}
					ln = buffer.readLine();
				}
			}
		}

		private Text pairFriends = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String userinfoLine = value.toString();

			String[] values = userinfoLine.split("\t");

			Configuration conf = context.getConfiguration();
			// getting pair values
			int inputA = Integer.parseInt(conf.get("FRIEND1_ID"));
			int inputB = Integer.parseInt(conf.get("FRIEND2_ID"));

			if (values.length == 2) {

				int user1 = Integer.parseInt(values[0]);

				// getting friends corresponding to user 1
				List<String> friendsList = Arrays.asList(values[1].split(","));

				for (String user : friendsList) {

					int user2 = Integer.parseInt(user);

					if ((user1 == inputA && user2 == inputB) || (user2 == inputA && user1 == inputB)) {
						// for output value
						StringBuilder str = new StringBuilder();

						// making sorted pair of users as key
						if (user1 < user2) {
							pairFriends.set(user1 + "," + user2);
						} else {
							pairFriends.set(user2 + "," + user1);
						}

						for (String friend : friendsList) {
							int friend_ID = Integer.parseInt(friend);

							// creating value to output as friend id : name :
							// date of birth
							str.append(friend_ID + ":" + map.get(friend_ID) + ",");
						}

						// removing last occurrence of ','
						if (str.lastIndexOf(",") > -1) {
							str.deleteCharAt(str.lastIndexOf(","));
						}

						// replicating the user info at mapper
						context.write(pairFriends, new Text(str.toString()));
					}
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// to find store friends that have occurred
			Set<Integer> set = new HashSet<Integer>();

			StringBuilder str = new StringBuilder(); // output
			str.append("[");

			// iterating over two lists for the pair
			for (Text friendsList : values) {
				String[] users = friendsList.toString().split(",");
				// iterating over all friends in a single list
				for (String user : users) {
					String[] userinfo = user.split(":");
					int id = Integer.parseInt(userinfo[0]);
					if (set.contains(id)) {
						// if set contains the user i.e mutual friend so append
						// the name and date of birth information to the
						// resultant string
						str.append(userinfo[1] + ":" + userinfo[2] + ",");
					} else {
						// add first occurrence of user to set
						set.add(id);
					}
				}
			}

			// removing last occurrence of ','
			if (str.lastIndexOf(",") > -1) {
				str.deleteCharAt(str.lastIndexOf(","));
			}
			str.append("]");

			context.write(key, new Text(str.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		if (otherArgs.length < 5) {
			System.out.println("Pass 5 arguments");
			System.exit(1);
		}

		// Input = <Input_Path_For_Common_Friends_File>
		// <Input_Path_For_User_Data_File> <ID_Friend1> <ID_Friend2>
		// <Output_Path>

		config.set("USERINFO", otherArgs[1]);
		config.set("FRIEND1_ID", otherArgs[2]);
		config.set("FRIEND2_ID", otherArgs[3]);

		Job job = Job.getInstance(config);

		job.setJarByClass(UserInfo.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// setting the input and output path from list of arguments
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
