import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.HashMap;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.*;


public class Likes {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        //mapper1 = tweet data

        // nine columns 
        // ID, username, full name, URL, timestamp, comments, likes, retweets
        // [0]    [1]       [2]     [3]      [4]       [5]     [6]      [7]  

        // separated by ","

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] valueStr = value.toString().split(",");

            if(valueStr.length == 8){

                String id = valueStr[0];
                String user = valueStr[1];
                String timestamp = valueStr[4];
                String likes = valueStr[6];

                String [] formattime = timestamp.split(" ");
                String time = formattime[0]; // yyyy-mm-dd

                String cc = user + "~" + time + "~" + likes;

                if(!cc.contains("likes")){
                    context.write(new Text(id), new Text(cc));
                }
            }
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        HashMap<String, List<String>> hm = new HashMap<String, List<String>>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> list = new ArrayList<String>();

            for(Text val : values){
                String [] valueStr = val.toString().split("~");

                String user = valueStr[0];
                String timestamp = valueStr[1];
                String likes = valueStr[2];

                list.add(likes);
                list.add(timestamp);
                list.add(user);
            }
            hm.put(key.toString(), list);
            
        }

        // hashmap: <key: id, value: [# likes, timestamp, user]>

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // sort hashmap in descending order based on first value in arraylist (# comments)
            List<Map.Entry<String,List<String>>> entries = new ArrayList<Map.Entry<String,List<String>>>(hm.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<String,List<String>>>() {
                public int compare(Map.Entry<String,List<String>> l1, Map.Entry<String,List<String>> l2) {
                    int val1 = Integer.parseInt(l1.getValue().get(0));
                    int val2 = Integer.parseInt(l2.getValue().get(0));
                    int comp = Integer.compare(val1, val2);
                    return (comp = -comp); //negative for descending
                }
            });

            int count = 0; 

            for(Map.Entry<String, List<String>> entry : entries){ // sorted entries

                String listvalues = "";

                for(String val : entry.getValue()){
                    listvalues += val;
                    listvalues += "\t"; // concat to single string with tab sep.
                }
                context.write(new Text(entry.getKey()), new Text(listvalues));

                count++;
                if(count == 100) {break;} 
            }
 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top 100 likes");
        job.setJarByClass(Likes.class);
        job.setMapperClass(Likes.Mapper1.class);
        job.setReducerClass(Likes.Reducer1.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}