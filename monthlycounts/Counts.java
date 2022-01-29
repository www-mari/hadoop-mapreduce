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


public class Counts {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        //mapper1 = tweet data

        // nine columns 
        // ID; username; full name; URL; timestamp; comment; likes; retweets
        // [0]    [1]       [2]     [3]      [4]       [5]     [6]      [7]

        // separated by ","

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String [] valueStr = value.toString().split(",");

            if(valueStr.length == 8){
                String id = valueStr[0];
                String timestamp = valueStr[4];

                String [] formattime = timestamp.split(" ");
                String mdy = formattime[0]; // yyyy-mm-dd
                String my = mdy.substring(0, mdy.length() - 3); //remove day -> yyyy-mm

                if(!my.contains("time")){
                    context.write(new Text(my), new Text("1"));
                }
            }
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        int count = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text val : values){
                count++;
            }

            context.write(new Text(key), new Text(Integer.toString(count)));

            // for (Text val : values){
            //     context.write(new Text(key), new Text(val));
            // }
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "monthly tweet counts");
        job.setJarByClass(Counts.class);
        job.setMapperClass(Counts.Mapper1.class);
        job.setReducerClass(Counts.Reducer1.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //tweets.csv
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}