package functionalModules;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;

/*************************
 * Determines the preferred sequence of mode of customer support for a
 * particular product
 *****************************/
public class ModePredictionFunctionality {
	static HashMap<String, String> averageModetoRatingMap = new HashMap<String, String>();
	static String userEnteredProduct;

	public ModePredictionFunctionality(String userEnteredProduct) {
		ModePredictionFunctionality.userEnteredProduct = userEnteredProduct.trim();
	}

	// This mapper will read all the records from the ProductComplaints.csv and
	// output with Product_Mode as key and consumer complaint rating as value.
	static public class ComplaintModeMapper extends Mapper<Object, Text, Text, Text> {
		int counter = 1;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				CSVParser parser = new CSVParser(',', '\"');
				String[] record = parser.parseLineMulti(value.toString());
				if (counter == 1) {
					counter = 2;
					return;
				}
				Text product = new Text(record[3]);
				Text mode = new Text(record[2]);
				Text rating = new Text(record[6]);
				String product_mode = product + "_" + mode;
				context.write(new Text(product_mode), rating);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This reducer will calculate the average rating for each of the modes for
	// every product
	static public class ComplaintModeReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				int count = 0;
				int sum = 0;
				String temp;
				for (Text v : values) {
					count++;
					sum = sum + Integer.parseInt(v.toString());
				}

				double average = (double) sum / (double) count;
				temp = String.valueOf(average);
				context.write(key, new Text(temp));
				averageModetoRatingMap.put(key.toString(), String.valueOf(average));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This method sorts and prints the order in which the different modes of
	// customer support should be advertised
	public static void modeTrendAnalyzer(String product) {
		if (averageModetoRatingMap == null || averageModetoRatingMap.isEmpty()) {
			return;
		}
		Comparator<String> comparator = new ValueComparator(averageModetoRatingMap);
		TreeMap<String, String> sortedMap = new TreeMap<String, String>(comparator);
		sortedMap.putAll(averageModetoRatingMap);// Sorting the averageModetoRatingMap for finding out the best mode for
													// a product
		int count = 0;
		for (String key : averageModetoRatingMap.keySet()) {
			String productName = key.split("_")[0];
			if (productName.toString().equalsIgnoreCase(product)) {
				count++;
			}
		}
		if (count == 0) {
			System.out.println("Invalid product name entered");
			return;
		}
		System.out.println();
		System.out.println();
		System.out.println(
				"Priority for different modes for " + product + " based on customer satisfaction is as follows: ");
		System.out.println("-----------------------------------------------------------------------------------------");
		for (String key : averageModetoRatingMap.keySet()) {
			String productName = key.split("_")[0];
			String mode = key.split("_")[1];
			if (productName.toString().equalsIgnoreCase(product)) {
				System.out.println(mode);
			}
		}

	}

	public void runJob(Configuration conf, String[] otherArgs) throws Exception {
		// Job to determine the preference of mode of customer support for particular
		// product
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Deduce Customer Complaints per product");
		job.setJarByClass(ModePredictionFunctionality.class);
		job.setInputFormatClass(NLineInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ComplaintModeMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));
		job.setReducerClass(ComplaintModeReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		modeTrendAnalyzer(userEnteredProduct);
	}
}
