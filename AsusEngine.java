import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.Logger;

import javax.xml.stream.*;
import java.io.*;

import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Collections;

import static javax.xml.stream.XMLStreamConstants.*;

public final class AsusEngine {
	
	private static final Logger log = Logger.getLogger(AsusEngine.class);

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text author = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
			String document = value.toString();
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
						case START_ELEMENT:
							currentElement = reader.getLocalName();
							break;
						case CHARACTERS:
							if (currentElement.equalsIgnoreCase("author")) {
								String auth = reader.getText();
								if (auth.length() > 1){
									author.set(auth);
									context.write(author, one);
								}
							}
						break;
					}
				}
				reader.close();
				author.set("_DOCUMENT_"); // count dokumen
				context.write(author, one);
			} catch (Exception e) {
				log.error("Error processing '" + document + "'", e);
			}
		}
	}
	
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private HashMap<Text, IntWritable> countMap = new HashMap<>();
		
		@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); 
            }
			
			// cari kvalue terkecil di countMap
			Text minKey = null; int minVal = 0;
			for (Text key1 : countMap.keySet()) {
				if (minKey == null || countMap.get(key1).get() < minVal){
					minVal = countMap.get(key1).get();
					minKey = key1;
				}
            }
			
			// kalo misalnya jumlah artikel yg d tulis penulis ini > artikel minimal di countMap
			if (sum > minVal){
				if (minKey != null && countMap.size() > 6){
					countMap.remove(minKey); // hapus kalo misalnya countmap masih belom penuh (disini kapasitas = 7) sehingga data yg d proses di clean up = N * 6
				}
				countMap.put(new Text(key), new IntWritable(sum));
			}

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			HashMap<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 6) {
                    break;
                }
				
                context.write(key, sortedMap.get(key));
            }
			
        }
    }

	public static void main(String... args) {
		try{
			runJob(args[0], args[1], args[2]);
		}catch(Exception e){
			log.error("Error MAIN", e);
		}
	}

	public static void runJob(String input, String output, String xmlTag) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", " ");
		conf.set("xmlinput.start", xmlTag);
		conf.set("xmlinput.end", "/" + xmlTag);

		Job job = new Job(conf);
		job.setJarByClass(AsusEngine.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		
		job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
		
		//job.setNumReduceTasks(0);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.waitForCompletion(true);
	}
	
	
	private static <K extends Comparable, V extends Comparable> HashMap<K, V> sortByValues(HashMap<K, V> HashMap) {
        List<HashMap.Entry<K, V>> entries = new LinkedList<HashMap.Entry<K, V>>(HashMap.entrySet());

        Collections.sort(entries, new Comparator<HashMap.Entry<K, V>>() {

            @Override
            public int compare(HashMap.Entry<K, V> o1, HashMap.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        HashMap<K, V> sortedHashMap = new LinkedHashMap<K, V>();

        for (HashMap.Entry<K, V> entry : entries) {
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }

        return sortedHashMap;
    }
}