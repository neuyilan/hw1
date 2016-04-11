

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * 
 * @author qihouliang
 *
 */
public class Hw1Grp3_integer {
	

	/**
	 * 
	 * @param parameter		
	 * @return
	 */
	public static List<String> calParameter(String parameter) {
		List<String> list = new ArrayList<String>();
		if ("count".equals(parameter)) {
			list.add("count");
			list.add(String.valueOf(Integer.MAX_VALUE));
		} else if ("avg".equals(parameter.substring(0, 3))) {
			list.add("avg");
			list.add(parameter.substring(5, 6));
		} else if ("max".equals(parameter.substring(0, 3))) {
			list.add("max");
			list.add(parameter.substring(5, 6));
		}
		return list;
	}

	/**
	 * 
	 * @param url		the hdfs url
	 * @param columnIndex	the column for groupby
	 * @param resultArray	the parameter for calculate
	 * @return  the calculate result
	 */
	public static TreeMap<String, Integer>[] read(String url, int columnIndex,
			String resultArray[]) {

		TreeMap<String, Integer>[] mediumMap = new TreeMap[resultArray.length];
		TreeMap<String, Integer> countMap = new TreeMap<String, Integer>();

		for (int i = 0; i < resultArray.length; i++) {
			mediumMap[i] = new TreeMap<String, Integer>();
		}

		String calStr[] = new String[resultArray.length];
		int calIndex[] = new int[resultArray.length];

		for (int i = 0; i < resultArray.length; i++) {
			List<String> paraList = calParameter(resultArray[i]);
			calStr[i] = paraList.get(0);
			calIndex[i] = Integer.parseInt(paraList.get(1));
		}

		String file = "hdfs://localhost:9000" + url;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(file), conf);
			Path path = new Path(file);
			FSDataInputStream in_stream = fs.open(path);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					in_stream));
			String str;
			String strArray[];

			while ((str = in.readLine()) != null) {
				strArray = str.split("\\|");
				String key = strArray[columnIndex];

				// calculate the count
				if (countMap.get(key) == null) {
					countMap.put(key, 1);
				} else {
					countMap.put(key, countMap.get(key) + 1);
				}

				for (int i = 0; i < resultArray.length; i++) {
					if (calStr[i].equals("count")) {
						mediumMap[i] = calCount(key, mediumMap[i]);
					} else if (calStr[i].equals("max")) {
						mediumMap[i] = calMax(key,
								Integer.parseInt(strArray[calIndex[i]]),
								mediumMap[i]);
					} else if (calStr[i].equals("avg")) {
						mediumMap[i] = calAvg(key,Integer.parseInt(strArray[calIndex[i]]),mediumMap[i]);
					}
				}
			}

			// calculate the avg value
			for (int i = 0; i < resultArray.length; i++) {
				if (calStr[i].equals("avg")) {
					Iterator<Entry<String, Integer>> avgIter = mediumMap[i]
							.entrySet().iterator();
					while (avgIter.hasNext()) {
						Entry<String, Integer> entry = avgIter.next();
						String key = entry.getKey();
						float val = (float) entry.getValue();
						int countValue = countMap.get(key);
						float avg = val / countValue;
						DecimalFormat df = new DecimalFormat("#.00");
						mediumMap[i].put(key, Integer.parseInt(df.format(avg)));
					}
				}
			}

			in.close();
			fs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return mediumMap;
	}

	// actually is calculate the sum
	private static TreeMap<String, Integer> calAvg(String key, int value,
			TreeMap<String, Integer> mediumMap) {
		if (mediumMap.get(key) == null) {
			mediumMap.put(key, value);
		} else {
			mediumMap.put(key, value + (int) mediumMap.get(key));
		}
		return mediumMap;
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 * @param mediumMap   
	 * @return  the max value for the key
	 */
	private static TreeMap<String, Integer> calMax(String key, int value,
			TreeMap<String, Integer> mediumMap) {
		if (mediumMap.get(key) == null || value > mediumMap.get(key)) {
			mediumMap.put(key, value);
		}
		return mediumMap;
	}

	/**
	 * 
	 * @param key
	 * @param mediumMap
	 * @return  the count value for the key
	 */
	private static TreeMap<String, Integer> calCount(String key,
			TreeMap<String, Integer> mediumMap) {
		if (mediumMap.get(key) == null) {
			mediumMap.put(key, 1);
		} else {
			mediumMap.put(key, 1 + mediumMap.get(key));
		}
		return mediumMap;
	}

	public static void write(TreeMap<String, Integer> resultMap[],
			String calStr[]) {
		// create table descriptor
		try {
			String tableName = "Result";
			HTableDescriptor htd = new HTableDescriptor(
					TableName.valueOf(tableName));
			// create column descriptor
			HColumnDescriptor cf = new HColumnDescriptor("res");
			htd.addFamily(cf);

			// configure HBase
			Configuration configuration = HBaseConfiguration.create();
			HBaseAdmin hAdmin = new HBaseAdmin(configuration);

			if (hAdmin.tableExists(tableName)) {
				hAdmin.disableTable(tableName);
				hAdmin.deleteTable(tableName);
			}

			hAdmin.createTable(htd);

			HTable table = new HTable(configuration, tableName);
			// sort by key, resultMap.size will always >0
			Object[] sortKey = resultMap[0].keySet().toArray();
			Arrays.sort(sortKey);
			Put put;
			for (int i = 0; i < sortKey.length; i++) {
				String key = (String) sortKey[i];
				put = new Put(key.getBytes());
				for (int j = 0; j < resultMap.length; j++) {
					put.add("res".getBytes(), calStr[j].getBytes(),
							resultMap[j].get(key).toString().getBytes());
				}

				table.put(put);
			}
			hAdmin.close();
			table.close();
			System.out.println("put successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (3 != args.length) {
			System.out.println("you input the wrong paramter");
			return;
		}

		String path = args[0].substring(2);
		
		// group by this column
		int columnIndex = Integer.parseInt(args[1].substring(9, 10));
		String result = args[2].substring(4);
		String resultArray[] = result.split(",");

		TreeMap<String, Integer> resultMap[] = read(path, columnIndex,
				resultArray);
		write(resultMap, resultArray);
	}

}

