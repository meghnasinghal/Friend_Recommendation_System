
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class FriendsMaxAge {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        	//split user and list of friends
            String[] userInfo = value.toString().split("\t");
            if (userInfo.length == 2) {
            	//output as user and list of friends
                context.write(new Text(userInfo[0]), new Text(userInfo[1]));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
    	
    	//to store user and his age
        static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration config = context.getConfiguration();
            FileSystem fs = FileSystem.get(config);
            FileStatus[] status_arr = fs.listStatus(new Path(config.get("USERDATA")));
            for (FileStatus file_status : status_arr) {
                BufferedReader buffer = new BufferedReader(new InputStreamReader(fs.open(file_status.getPath())));
                String ln = buffer.readLine();
                while (ln != null) {
                    String[] split = ln.split(",");
                    if (split.length == 10) {
                        int age = 0;
                        try {
                            age = getAge(split[9]);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        //put in map user id and age
                        map.put(Integer.parseInt(split[0]), age);
                    }
                    ln = buffer.readLine();
                }
            }
        }
        
        SimpleDateFormat simple = new SimpleDateFormat("M/d/yyyy");
        DateFormat date = new SimpleDateFormat("yyyyMMdd");
		// Age calculated based on today's date and the date of birth from userdata.txt
        public int getAge(String dateStr) throws ParseException {

            int dob = Integer.parseInt(date.format(simple.parse(dateStr)));
            int today = Integer.parseInt(date.format(new Date()));
            //calculating age of user
            int age = (today - dob) / 10000;
            return age;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (Text friendList: values) {
                String[] users = friendList.toString().split(",");
                for (String user: users){
                	if(map.containsKey(Integer.parseInt(user)))
                	{ 
                		int age = map.get(Integer.parseInt(user));
                		//calculating max age from all direct friends
                		max = Math.max(max,age);
                	}
                   
                }
            }
            context.write(key, new Text(Integer.toString(max)));
        }
    }

   

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        config.set("USERDATA", otherArgs[1]);

        Job job = Job.getInstance(config);

        job.setJarByClass(FriendsMaxAge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
