package dataHandler;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class LoadData {

        public static void main(String[] args) throws Exception {
                			LoadData ld=new LoadData();
                			try{
                				ld.makedir("hdfs://localhost:8020/BankingTrendAnalyzer");
                				String[] arg = {"ProductComplaints.csv","hdfs://localhost:8020/BankingTrendAnalyzer/ProductComplaints.csv","CustomerProductData.csv","hdfs://localhost:8020/BankingTrendAnalyzer/CustomerProductData.csv", 
                						"CustomerServiceCosting.csv","hdfs://localhost:8020/BankingTrendAnalyzer/CustomerServiceCosting.csv"}; 
                    			ld.load(arg);
                			}
                			catch(IOException ioException) {
                				ioException.printStackTrace();
                				System.out.println("Caught exception while making Container on Hadoop database");
                				
                			}
                			catch(Exception e) {
                				e.printStackTrace();
                				System.out.println("Caught an exception while loading data into Hadoop");
                			}
                			
                			
                			
                			
        } 
        public void makedir(String directory) throws IOException
        {
        		org.apache.hadoop.conf.Configuration con =new org.apache.hadoop.conf.Configuration();
        		System.out.println("************Establishing Connection");
        		con.set("fs.default.name","hdfs://localhost:8020");
        		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(con);
        		System.out.println("*************Connection Established");
        		Path pth=new Path(directory);
        		fs.mkdirs(pth);
        		System.out.println("**************Created container in the Database named: BankingTrendAnalyzer");
        		fs.close();
        		System.out.println("**************Connection Closed");
        		
        }
       
        public int load(String[] args) throws Exception {
            
            if (args.length < 2) {
                System.err.println("HdfsReader [local input path] [hdfs output path]");
                return 1;
            }
            
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            Path inputPath2 = new Path(args[2]);
            Path outputPath2 = new Path(args[3]);
            Path inputPath3 = new Path(args[4]);
            Path outputPath3 = new Path(args[5]);
            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://localhost:8020");
            org.apache.hadoop.fs.FileSystem fs= org.apache.hadoop.fs.FileSystem.get(conf);
            System.out.println("************Connection Established");
            fs.copyFromLocalFile(inputPath, outputPath);
            System.out.println("************CopiedFirstCsv");
            fs.copyFromLocalFile(inputPath2, outputPath2);
            System.out.println("************CopiedSecondCsv");
            fs.copyFromLocalFile(inputPath3, outputPath3);
            System.out.println("************CopiedThirdCsv");
            System.out.println("************Successfully uploaded the dataset to Hadoop Database");
            return 0;
        }
        
}