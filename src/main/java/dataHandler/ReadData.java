package dataHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
public class ReadData {

        public static void main(String[] args) throws Exception {
                			ReadData rd=new ReadData();
                			String[] arg = {"hdfs://localhost:8020/BankingTrendAnalyzer/Consumer_Complaint_DataSet_Extract.csv","output.csv"};
                			try {
                				rd.read(arg);
                			}
                			catch(Exception e) {
                				e.printStackTrace();
                				System.out.println("Caught Exception while reading dataset from Hadoop");
                			}
                			
                			
                			
        } 
        public int read(String[] args) throws Exception {
            
            if (args.length < 2) {
                System.err.println("HdfsReader [hdfs input path] [local output path]");
                return 1;
            }
            
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://localhost:8020");
            org.apache.hadoop.fs.FileSystem fs= org.apache.hadoop.fs.FileSystem.get(conf);
            System.out.println("############Connection Established");
            fs.copyToLocalFile(inputPath, outputPath);
            System.out.println("############Successfully downloaded the file. please refresh the project");
            return 0;
        }
        
}