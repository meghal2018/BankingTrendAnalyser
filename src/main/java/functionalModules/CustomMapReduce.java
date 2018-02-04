package functionalModules;

import java.net.URI;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CustomMapReduce {

	static int CompaniesErrorRate = 10; // Assumption: 10% of the total number of customers is the companies
										// expected complaint count for each product
	static String userEnteredProduct = "";
	static String userEnteredMonth = "";

	/*************************
	 * Main Method
	 *****************************/
	public static void main(String[] args) throws Exception {
		String arg[] = { "hdfs://localhost:8020/BankingTrendAnalyzer/ProductComplaints.csv",
				"hdfs://localhost:8020/BankingTrendAnalyzer/CustomerProductData.csv",
				"hdfs://localhost:8020/BankingTrendAnalyzer/CustomerServiceCosting.csv",
				"hdfs://localhost:8020/BankingTrendAnalyzer/ProductComplaintMap",
				"hdfs://localhost:8020/BankingTrendAnalyzer/AverageCustomerRatingandCostMap",
				"hdfs://localhost:8020/BankingTrendAnalyzer/AverageRatingModeMap" };
		Configuration conf = new Configuration();
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 1000);
		String[] otherArgs = new GenericOptionsParser(conf, arg).getRemainingArgs();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
		Scanner sc = new Scanner(System.in);
		int option = 0;
		loop: while (true) {
			System.out.println();
			System.out.println("Enter 1 to see pattern of customer to complaint ratio");
			System.out.println(
					"Enter 2 to see performance of support team for each feature of a product in a particular month ");
			System.out
					.println("Enter 3 to see the preffered sequence of modes for a product based on the historic data");
			System.out.println("Enter 4 to change the default location of mappers in HDFS");
			System.out.println("Enter 5 to change the default error rate for the company");
			System.out.println("Enter any other number to exit");
			Path output1 = new Path(otherArgs[3]);
			Path output2 = new Path(otherArgs[4]);
			Path output3 = new Path(otherArgs[5]);
			try {
				option = Integer.parseInt(sc.nextLine());

			} catch (Exception e) {
				System.out.println("Invalid Input, menu is number driven. Run the application again.");
				return;
			}
			switch (option) {
			case 1:
				if (hdfs.exists(output1)) {
					hdfs.delete(output1, true);
				}
				CustomerComplaintRatioFunctionality complaintRatioFunctionality = new CustomerComplaintRatioFunctionality(
						CompaniesErrorRate);
				complaintRatioFunctionality.runJob(conf, otherArgs);
				break;

			case 2:
				if (hdfs.exists(output2)) {
					hdfs.delete(output2, true);
				}
				System.out.println("Enter product name");
				userEnteredProduct = sc.nextLine();
				System.out.println("Enter month for analysis [within the range 1/13 to 7/13] ");
				userEnteredMonth = sc.nextLine();
				FeatureSupportFunctionality featureSupportFunctionality = new FeatureSupportFunctionality(
						userEnteredProduct, userEnteredMonth);
				featureSupportFunctionality.runJob(conf, otherArgs);
				break;

			case 3:
				if (hdfs.exists(output3)) {
					hdfs.delete(output3, true);
				}
				System.out.println("Enter product name");
				userEnteredProduct = sc.nextLine();
				ModePredictionFunctionality modePredictionFunctionality = new ModePredictionFunctionality(
						userEnteredProduct);
				modePredictionFunctionality.runJob(conf, otherArgs);
				break;

			case 4:
				System.out.println("Enter the location for Product Complaint Map");
				otherArgs[3] = sc.nextLine();
				System.out.println("Enter the location for Average Customer Rating and Cost Map");
				otherArgs[4] = sc.nextLine();
				System.out.println("Enter the location for Average Rating Mode Map");
				otherArgs[5] = sc.nextLine();
				System.out.println("Successfully updated the map locations");
				break;

			case 5:
				System.out.println("Enter the new companies standard error percentage");
				try {
					CompaniesErrorRate = Integer.parseInt(sc.nextLine());

				} catch (Exception e) {
					System.out.println("Invalid Input");
					return;
				}
				
				break;
			default:
				System.out.println("System exited");
				break loop;
			}
		}
		sc.close();
	}
}