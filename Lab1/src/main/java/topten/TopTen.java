package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {

    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            // System.err.println(xml);
        }

        return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, org.apache.hadoop.io.NullWritable, Text> {
        // Stores a map of user reputation to the record
        TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> map = transformXmlToMap(value.toString());

            if ((map.get("Reputation") != null ) && (map.get("Id") != null)){

                String id = map.get("Id");
                int rep = Integer.parseInt(map.get("Reputation"));

                repToRecordMap.put(rep, value);

                context.write(NullWritable.get(), value);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            int cnt = 0;

            for (Map.Entry<Integer, Text> elem: repToRecordMap.descendingMap().entrySet()) {

                context.write(NullWritable.get(), elem.getValue());
                cnt++;
                if(cnt == 10) {
                    break;
                }
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text elem : values) {

                Map<String, String> row = transformXmlToMap(elem.toString());

                if ((row.get("Id") != null) && (row.get("Reputation") != null)) {

                    int rep = Integer.parseInt(row.get("Reputation"));
                    Text id = new Text(row.get("Id"));

                    repToRecordMap.put(rep, id);
                }

            }

            int cnt = 0;
            for (Map.Entry<Integer, Text> elem : repToRecordMap.descendingMap().entrySet()) {

                Text id = elem.getValue();
                int rep = elem.getKey();

                Put insHBase = new Put(Bytes.toBytes(cnt));

                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(Integer.toString(rep)));
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id.toString()));

                context.write(NullWritable.get(), insHBase);
                cnt++;
                if(cnt == 10) {
                    break;
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "topten");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));

        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}