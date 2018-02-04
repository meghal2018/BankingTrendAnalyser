package functionalModules;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

public class ValueComparator implements Comparator<String>{
	 
		HashMap<String, String> map = new HashMap<String, String>();
	 
		public ValueComparator(HashMap<String, String> map){
			this.map.putAll(map);
		}
		
		public ValueComparator() {
			
		}
	 
		@Override
		public int compare(String s1, String s2) {
			double v1 = Double.parseDouble(map.get(s1));
			double v2 = Double.parseDouble(map.get(s2));
			if(v1 >= v2 ){
				return -1;
			}else{
				return 1;
			}	
		}
	}