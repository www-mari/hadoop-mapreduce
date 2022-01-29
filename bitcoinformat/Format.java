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
import java.util.Calendar;


public class Format {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        //mapper1 = bitcoin data

        // seven columns
        // date, price, open, high, low, vol., change %
        // [0]    [1]   [2]   [3]   [4]  [5]     [6]
        
        //separated by ","

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] valueStr = value.toString().split("\",\"");

            if(valueStr.length == 7){
                String timestamp = valueStr[0];
                String price = valueStr[1];


                try {
                    //change date format to match tweets.csv dataset
                    final String OLD_FORMAT = "MMM dd, yyyy";
                    final String NEW_FORMAT = "yyyy-MM-dd";

                    String olddate = timestamp.substring(1, timestamp.length()); //remove beginning quotes

                    SimpleDateFormat sdf = new SimpleDateFormat(OLD_FORMAT);
                    Date d = sdf.parse(olddate);
                    sdf.applyPattern(NEW_FORMAT);
                    String newdate = sdf.format(d);

                    context.write(new Text(newdate), new Text(price));
                }
                catch (Exception e) {
                    context.write(new Text("failed"), new Text("1"));
                }

                //context.write(new Text("test"), new Text(timestamp));
            }
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if(!key.toString().contains("Date")){
                for(Text val : values){
                    context.write(new Text(key), val);
                }
            } else {
                //context.write(new Text("failed"), new Text("1"));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        //mapper2 = formatted bitcoin data

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] valueStr = value.toString().split("\t");

            if(valueStr.length == 2){
                String date = valueStr[0];
                String price = valueStr[1];

                context.write(new Text(date), new Text(price));
            }

            
        }

    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        HashMap<String, String> hm = new HashMap<String, String>();
        HashMap<String, String> hmlist = new HashMap<String, String>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text val : values){
                hm.put(key.toString(), val.toString());
                //context.write(new Text(key), new Text(val));
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for(Map.Entry<String, String> entry : hm.entrySet()){
                String startdate = entry.getKey();
                String prices = entry.getValue();

                for(int i = 0; i < 7; i++){
                    prices += " | ";

                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Calendar c = Calendar.getInstance();
                        c.setTime(sdf.parse(startdate));
                        c.add(Calendar.DATE, 1);  // number of days to add
                        String nextdate = sdf.format(c.getTime());  // nextdate is now 1 day after startdate

                        if(hm.containsKey(nextdate)){
                            prices += hm.get(nextdate);
                        }
                        startdate = nextdate;
                    }
                    catch (Exception e){
                    }
                }
                
                hmlist.put(entry.getKey(), prices);

            }

            for(Map.Entry<String, String> entry : hmlist.entrySet()){
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "format bitcoin date");
        job.setJarByClass(Format.class);
        job.setMapperClass(Format.Mapper1.class);
        job.setReducerClass(Format.Reducer1.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "add 7 days of prices");
        job1.setJarByClass(Format.class);
        job1.setMapperClass(Format.Mapper2.class);
        job1.setReducerClass(Format.Reducer2.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);        

    }
}