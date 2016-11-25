import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.*;

public class HW02 {
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		Log log = LogFactory.getLog(MyMapper.class);
		private Text station = new Text();
		private Text one = new Text("1");

		@Override
		public void map(LongWritable line, Text inputText, OutputCollector<Text, Text> collector, Reporter arg3) throws IOException {
			String inputString = inputText.toString();
			String[] inputs = inputString.split("[\\W+]");
			int hour = 100000, min = 1000000;

			try {
				hour = Integer.parseInt(inputs[2]);
				min = Integer.parseInt(inputs[3]);
			}catch(Exception e) {
				log.error("<<<<<<<<<<<<<<<<<<<<<<Split err>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				for(int i = 0 ; i < inputs.length; i++)
					log.info(i + " : " + inputs[i]);
				log.info("<<<<<<<<<<<<<<<<<<<<<<Error end>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				return;
			}
			if((hour < 6) || ((hour == 6)&&(min == 0))) {
				String stationInString = inputs[4];
				station.set(stationInString);
				collector.collect(station,one);
				log.info("MyMapper : (" + station.toString() + ", " + "1)collected");
			}
		}
	}

	public static class Combiner extends MapReduceBase implements Reducer<Text ,Text , Text, Text> {
		Log log = LogFactory.getLog(Combiner.class);
		
		@Override
		public void reduce(Text inputStation, Iterator<Text> ones, OutputCollector<Text, Text> collector, Reporter arg3) throws IOException {
			long sum = 0;

			while(ones.hasNext()) {
				ones.next();
				sum += 1;
			}

			Text sumInText = new Text(String.valueOf(sum));
			collector.collect(sumInText, inputStation);
			log.info("Combine : (" + sumInText.toString() + ", " + inputStation.toString() +")collected");
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		String four_Stations = "";
		int[] distanceArray = {700, 1500,800, 1200, 900, 800, 1300};
		HashMap<String, Long> map = new HashMap<String, Long>();
		Log log = LogFactory.getLog(Reduce.class);
		OutputCollector<Text,Text> closingCollector = null;

		private String sumDistance(char start, char end){
			int distance = 0;
			int startIndex, endIndex;
			startIndex = (int)start - 65;
			endIndex = (int)end - 65;

			if(startIndex < endIndex) {
				for(int i = startIndex; i < endIndex; i++)
					distance += distanceArray[i];
			} else {
				for(int i = startIndex - 1; i >= endIndex; i--)
					distance += distanceArray[i];
			}

			return String.valueOf(distance);
		}
		

		public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
			Comparator<K> valueComparator = new Comparator<K>() {
				public int compare(K k1, K k2) {
					int compare = 0;
					
					if(map.get(k1).compareTo(map.get(k2)) == -1)
						compare = 1;
					else if(map.get(k1).compareTo(map.get(k2)) == 1)
						compare = -1;

					if (compare == 0)
						return 1;
					else
						return compare;
				}
			};

			Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
			sortedByValues.putAll(map);

			return sortedByValues;
		}

		@Override
		public void reduce(Text frequency, Iterator<Text> stations, OutputCollector<Text, Text> collector, Reporter r) throws IOException {
			closingCollector = collector;
			String station;
			Long tmp = Long.valueOf(0);

			while(stations.hasNext()){
				station = stations.next().toString();
				tmp = Long.parseLong(frequency.toString());

				if(map.get(station) != null) {
					Long oldValue = map.get(station);
					map.remove(station);
					map.put(station, oldValue + tmp);
				} else {
					map.put(station, tmp);
				}

				log.info("Reduce : stations-" + station + "   Value-" + map.get(station));
			}
		}

		@Override
		public void close(){
			String[] result_Array = new String[4];
			Map<String, Long> sortedMap = sortByValues(map);
			Iterator iterator = sortedMap.entrySet().iterator();
			Map.Entry<String, Long> entry;

			while(iterator.hasNext()) {
				entry = (Map.Entry<String, Long>)iterator.next();
				log.info("Reduce : Key-" + entry.getKey() + "   Value-" + entry.getValue());
			}

			iterator = sortedMap.keySet().iterator();

			for(int i =0 ; i< 4; i++) {
				if(iterator.hasNext())
					result_Array[i] = (String)iterator.next();
			}

			Arrays.sort(result_Array);

			for(int i = 0 ; i < 4; i++)
				four_Stations += result_Array[i];

			String d1 = sumDistance(four_Stations.charAt(0), four_Stations.charAt(1));
			String d2 = sumDistance(four_Stations.charAt(1), four_Stations.charAt(2));
			String d3 = sumDistance(four_Stations.charAt(2), four_Stations.charAt(3));

			String key = "정류장 순서는 " + four_Stations + "이며 ";
			String valueToString = "각 정류장 간의 거리는 " + d1 + ", " + d2 + ", " + d3 + "m";
			
			try {
				closingCollector.collect(new Text(key),new Text(valueToString));
			} catch (IOException e) {
				log.info("[----------------Collect ERROR----------------]\n" +e.getMessage());
			}

			log.info(key + " " + valueToString);
		}
	}

	public static void main(String... args) throws Exception {
		JobConf conf = new JobConf(HW02.class);
		conf.setJobName("LNB");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
	}
}