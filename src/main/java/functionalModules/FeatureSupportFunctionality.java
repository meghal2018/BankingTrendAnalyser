package functionalModules;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
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
 * Analysis of efficiency of Customer Support for every feature of a particular
 * product
 *****************************/
public class FeatureSupportFunctionality {
	static HashMap<String, String> averageComplaintRatingandCostMap = new HashMap<String, String>();
	static double maxRatio = 0;
	static String feature;
	static String userEnteredProduct = "";
	static String userEnteredMonth = "";

	public FeatureSupportFunctionality(String userEnteredProduct, String userEnteredMonth) {
		FeatureSupportFunctionality.userEnteredProduct = userEnteredProduct.trim();
		FeatureSupportFunctionality.userEnteredMonth = userEnteredMonth.trim();
	}

	// This mapper will read all the records from the ProductComplaints.csv and
	// output with Product feature as key and customer complaint rating and customer
	// service response time as value
	public static class ComplaintRatingandResponseTimeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				CSVParser parser = new CSVParser(',', '\"');
				String[] record = parser.parseLineMulti(value.toString());
				Text product = new Text(record[3]);
				Text feature = new Text(record[4]);
				String rating = record[6];
				String responseTime = record[5];
				responseTime = "RT_" + responseTime;
				rating = "RA_" + rating;
				if (!record[1].contains("/")) {
					return;
				}
				String dateformatted = record[1].split("/")[0] + "/" + record[1].split("/")[2];
				if (!(product.toString().equalsIgnoreCase(userEnteredProduct)
						&& dateformatted.equalsIgnoreCase(userEnteredMonth)))
					return;
				context.write(new Text(feature), new Text(rating));
				context.write(new Text(feature), new Text(responseTime));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This mapper will read all the records from the CustomerServiceCosting.csv and
	// output with feature as key and cost(for servicing complaint of that feature)
	// as value.
	public static class ComplaintCostPerProductMapper extends Mapper<Object, Text, Text, Text> {
		int counter = 1;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				CSVParser parser = new CSVParser(',', '\"');
				String[] record = parser.parseLineMulti(value.toString());
				Text product = new Text(record[0]);
				Text feature = new Text(record[1]);
				Text costPerFeature = new Text("COST_" + record[2]);

				if (counter == 1) {
					counter = 2;
					return;
				}
				if (!(product.toString().equalsIgnoreCase(userEnteredProduct)))
					return;
				context.write(new Text(feature), costPerFeature);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This reducer will take output from ComplaintRatingandResponseTimeMapper and
	// ComplaintCostPerProductMapper as input and creates a Hash map with
	// (Key,Value) as (Feature, AverageRating_AverageCost)
	public static class AverageComplaintRatingandCost extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				int sumRT = 0, sumRA = 0, countRT = 0, countRA = 0, cost = 10;
				Double averageRT, averageRA, averageCost, ratio;
				for (Text v : values) {
					String data = v.toString();
					String type = data.split("_")[0];
					String value = data.split("_")[1];

					if (type.equalsIgnoreCase("RT")) {
						countRT++;
						sumRT = sumRT + Integer.parseInt(value);
					} else if (type.equalsIgnoreCase("RA")) {
						countRA++;
						sumRA = sumRA + Integer.parseInt(value);
					} else if (type.equalsIgnoreCase("COST")) {
						cost = Integer.parseInt(value);
					} else {
						System.out.println(
								"Invalid type of data for key: " + key + ", the value is: " + value);
						return;
					}
				}
				if (countRA == 0) {
					return;
				}
				if (countRT == 0) {
					return;
				}
				averageRA = (double) sumRA / (double) countRA;
				averageRT = (double) sumRT / (double) countRT;
				averageCost = averageRT * cost;
				averageComplaintRatingandCostMap.put(key.toString(),
						averageRA.toString() + "_" + averageCost.toString());
				ratio = averageRA / averageCost;
				if (ratio > maxRatio) {
					maxRatio = ratio;
					feature = key.toString();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Compares all the feature's customer support in terms of rating and cost with
	// their average values for the product
	public static void analysisOfFeaturePerProduct() {
		if (feature == null || feature.isEmpty()) {
			System.out.println("Invalid input, please enter correct input for product name and month");
			return;
		}
		System.out.println();
		System.out.println("ANALYSIS BASED ON RATING TO COST RATIO");
		System.out.println("-----------------------------------------------------------------------------------------");
		System.out.println(
				"The best functionality for " + userEnteredProduct + " in " + userEnteredMonth + " is " + feature);
		int count = 0;
		Double sumRating = 0.0, sumCost = 0.0;
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		for (String feature : averageComplaintRatingandCostMap.keySet()) {
			sumRating = sumRating + Double.parseDouble(averageComplaintRatingandCostMap.get(feature).split("_")[0]);
			sumCost = sumCost + Double.parseDouble(averageComplaintRatingandCostMap.get(feature).split("_")[1]);
			count++;
		}
		Double averageRating = sumRating / count;
		Double averageCost = sumCost / count;
		for (String feature : averageComplaintRatingandCostMap.keySet()) {
			int printCounter = 0;
			String data = averageComplaintRatingandCostMap.get(feature);
			Double rating = Double.parseDouble(data.split("_")[0]);
			Double cost = Double.parseDouble(data.split("_")[1]);
			Double ratingDeviation = ((averageRating - rating) / averageRating) * 100;
			if (ratingDeviation > 0) {
				String ratingDev = df.format(ratingDeviation);
				System.out.print(feature + " is rated " + ratingDev + "% less than average, ");
				printCounter++;
			}
			Double costDeviation = ((cost - averageCost) / averageCost) * 100;
			if (costDeviation > 0) {
				String costDev = df.format(costDeviation);
				System.out.print(feature + " costs " + costDev + "% more than average");
				printCounter++;
			}
			if (printCounter != 0) {
				System.out.println();
				printCounter = 0;
			}
		}

	}

	public static void resetValue() {
		averageComplaintRatingandCostMap.clear();
		maxRatio = 0;
		feature=null;
	}

	public void runJob(Configuration conf, String[] otherArgs) throws Exception {
		@SuppressWarnings("deprecation")
		Job averageRatingCostJob = new Job(conf, "Deduce Average Customer Ratings and Cost per complaint");
		averageRatingCostJob.setJarByClass(FeatureSupportFunctionality.class);
		averageRatingCostJob.setInputFormatClass(NLineInputFormat.class);
		MultipleInputs.addInputPath(averageRatingCostJob, new Path(otherArgs[0]), TextInputFormat.class,
				ComplaintRatingandResponseTimeMapper.class);
		MultipleInputs.addInputPath(averageRatingCostJob, new Path(otherArgs[2]), TextInputFormat.class,
				ComplaintCostPerProductMapper.class);
		FileOutputFormat.setOutputPath(averageRatingCostJob, new Path(otherArgs[4]));
		averageRatingCostJob.setReducerClass(AverageComplaintRatingandCost.class);
		averageRatingCostJob.setOutputKeyClass(Text.class);
		averageRatingCostJob.setOutputValueClass(Text.class);
		averageRatingCostJob.waitForCompletion(true);
		analysisOfFeaturePerProduct();
		resetValue();

	}

}
