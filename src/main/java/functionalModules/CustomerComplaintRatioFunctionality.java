package functionalModules;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

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
 * Trend Analyzer for month on month product performance based on the customer
 * complaints
 *****************************/
public class CustomerComplaintRatioFunctionality {

	static int CompaniesErrorRate = 10; // Assumption: 10% of the total number of customers is the companies
	// expected complaint count for each product
	static HashMap<String, String> productComplaintRatioMap = new HashMap<String, String>();

	public CustomerComplaintRatioFunctionality(int CompaniesErrorRate) {
		CustomerComplaintRatioFunctionality.CompaniesErrorRate = CompaniesErrorRate;
	}

	// This mapper will read all the records from ProductComplaints.csv and
	// output with Product_Month as key and product as value.
	public static class ActualCustomerComplaintMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				CSVParser parser = new CSVParser(',', '\"');
				String[] record = parser.parseLineMulti(value.toString());

				String product = record[3];

				if (!record[1].contains("/")) {
					return;
				} else {
					String dateformatted = record[1].split("/")[0] + "/" + record[1].split("/")[2];
					dateformatted = product + "_" + dateformatted;
					product = "A_" + product;
					context.write(new Text(dateformatted), new Text(product));

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This mapper will read all the records from CustomerProductData.csv and output
	// with Product_Month as key and expected number of complaints as value
	public static class ExpectedCustomerComplaintMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {
				CSVParser parser = new CSVParser(',', '\"');
				String[] record = parser.parseLineMulti(value.toString());
				Text product = new Text(record[0]);
				if (!record[2].contains("/")) {
					return;
				} else {
					int customerCountExpected = Integer.parseInt(record[1]);
					int expectedComplaintCount = customerCountExpected * CompaniesErrorRate / 100;
					String dateformatted = record[2];
					dateformatted = product + "_" + dateformatted;
					String expectedComplaintCounter = "E_" + String.valueOf(expectedComplaintCount);
					context.write(new Text(dateformatted), new Text(expectedComplaintCounter));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// This reducer will take the output of ActualCustomerComplaintMapper &
	// ExpectedCustomerComplaintMapper as input and calculates the ratio of
	// complaints of each product for every month
	public static class ComplaintCount extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				int actualErrorCount = 0;
				int expectedErrorCount = 0;
				for (Text v : values) {
					String data = v.toString();
					String type = data.split("_")[0];
					String value = data.split("_")[1];
					if (type.equalsIgnoreCase("A")) {
						actualErrorCount++;
					} else if (type.equalsIgnoreCase("E")) {
						expectedErrorCount = Integer.parseInt(value);
					} else {
						System.out.println("F1Reducer::Invalid Data for Key: " + key + ", Data is: " + data);
						return;
					}
				}
				if (expectedErrorCount == 0) {
					System.out.println("F1Reducer::Expeced Error count missing for key: " + key);
				}
				DecimalFormat df = new DecimalFormat();
				df.setMaximumFractionDigits(2);
				Double ratio = (double) (expectedErrorCount - actualErrorCount) / (double) expectedErrorCount;
				productComplaintRatioMap.put(key.toString(), df.format(ratio));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Prints the analysis in a specific table format
	public static void printTrendAnalyzerOnConsole() {
		SortedSet<String> date = new TreeSet<String>();
		SortedSet<String> product = new TreeSet<String>();
		String blank = "";
		for (String key : productComplaintRatioMap.keySet()) {
			String productName = key.split("_")[0];
			String month = key.split("_")[1];
			date.add(month);
			product.add(productName);
		}
		System.out.println();
		System.out.println();
		System.out.println("THIS IS THE REPORT FOR ALL THE PRODUCTS");
		System.out.println("-----------------------------------------------------------------------------------------");
		Iterator<String> it = date.iterator();
		Iterator<String> it1 = product.iterator();

		System.out.print(String.format("%-30s", blank));
		while (it.hasNext()) {
			System.out.print(String.format("%-20s", it.next()));
		}
		System.out.println();
		while (it1.hasNext()) {
			String thisproduct = it1.next();
			System.out.print(String.format("%-30s", thisproduct));
			Iterator<String> it2 = date.iterator();
			while (it2.hasNext()) {
				String thisdate = it2.next();
				String key = thisproduct + "_" + thisdate;
				String thisvalue = productComplaintRatioMap.get(key);
				System.out.print(String.format("%-20s", thisvalue));
			}
			System.out.println();
		}
		System.out.println();
	}

	@SuppressWarnings("deprecation")
	public void runJob(Configuration conf, String[] otherArgs) throws Exception {
		// Job to find Actual to Expected Complaint Ratio for every product on a monthly
		// basis
		Job complaintComparisonJob = new Job(conf, "Deduce Customer Complaints per product");
		complaintComparisonJob.setJarByClass(CustomerComplaintRatioFunctionality.class);
		complaintComparisonJob.setInputFormatClass(NLineInputFormat.class);
		MultipleInputs.addInputPath(complaintComparisonJob, new Path(otherArgs[0]), TextInputFormat.class,
				ActualCustomerComplaintMapper.class);
		MultipleInputs.addInputPath(complaintComparisonJob, new Path(otherArgs[1]), TextInputFormat.class,
				ExpectedCustomerComplaintMapper.class);
		FileOutputFormat.setOutputPath(complaintComparisonJob, new Path(otherArgs[3]));
		complaintComparisonJob.setReducerClass(ComplaintCount.class);
		complaintComparisonJob.setOutputKeyClass(Text.class);
		complaintComparisonJob.setOutputValueClass(Text.class);
		complaintComparisonJob.waitForCompletion(true);
		printTrendAnalyzerOnConsole();

	}

}
