package tianyu;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

class Param {
	public static final boolean isDebug = false;
}

class HBase {
	/**
	 * Get hbase admin.
	 * @return
	 */
	public static HBaseAdmin getHbaseAdmin() {
		// configure HBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hbaseAdmin = null;
		try {
			hbaseAdmin = new HBaseAdmin(configuration);
		} catch (IOException e) {
			if (Param.isDebug) {
				System.err.println("[getBaseAdminError]: get hbaseAdmin error.");
			}
		}
		return hbaseAdmin;
	}
	
	
    /** 
     * Create a table.
     * @param tableName 
     */  
    public static void createTable(String tableName, String columnFamily) {
		try {
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
			
			// create column descriptor
			HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
			htd.addFamily(cf);

			// configure HBase
			HBaseAdmin hAdmin = getHbaseAdmin();
			hAdmin.createTable(htd);
			hAdmin.close();
		} catch (IOException e) {
			if (Param.isDebug) {
				System.err.println("[CreateTableError]: Error occor while creating a table.");
//				e.printStackTrace();
			}
		}  
    }
	
	
    /** 
     * Delete a table.
     * @param tableName 
     */  
    public static void dropTable(String tableName) {
		try {
			HBaseAdmin admin = getHbaseAdmin();
	        admin.disableTable(tableName);  
	        admin.deleteTable(tableName);
	        admin.close();
		} catch (TableNotFoundException e) {
			if (Param.isDebug) {
				System.err.println("[DeleteTableError]: The table is not exist.");
//				e.printStackTrace();
			}
        } catch (MasterNotRunningException e) {
			if (Param.isDebug) {
				System.err.println("[DeleteTableError]: The master is not running.");
//				e.printStackTrace();
			}
        } catch (ZooKeeperConnectionException e) {
			if (Param.isDebug) {
				System.err.println("[DeleteTableError]: The ZooKeeper can't be connectioned.");
//				e.printStackTrace();
			}
        } catch (IOException e) {
			if (Param.isDebug) {
				System.err.println("[DeleteTableError]: IOException");
//				e.printStackTrace();
			}
		}
    }

    /**
     * Put data into hbase.
     * @param tableName
     * @param datas
     */
    public static void put(String tableName, ArrayList<String[]> datas) {
		Configuration configuration = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(configuration, tableName);
		} catch (IOException e) {
			if (Param.isDebug) {
				System.err.println("[PutError]: Connect to table error.");
//				e.printStackTrace();
			}
		}
		try {
			for (String[] data: datas) {
				Put put = new Put(data[0].getBytes());
				put.add(data[1].getBytes(), data[2].getBytes(), data[3].getBytes());
				table.put(put);
			}
			table.close();
		} catch (IOException e) {
			if (Param.isDebug) {
				System.err.println("[PutError]: Add entry error.");
//				e.printStackTrace();
			}
		}
    }
}

class HDFS {
	/**
	 * Read a file from hdfs.
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<String> readFromHdfs(String root, String file) throws IOException {
		String filePath = root + file;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		FSDataInputStream in_stream = fs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		
		ArrayList<String> ret = new ArrayList<String>();
		String line = null;
		while ((line = in.readLine()) != null) {
			ret.add(line);
		}
		in.close();
		fs.close();
		return ret;
	}
}

public class Hw1Grp0 {
	static class Pair<S, T> {
		Pair(S first, T second) {
			this.first = first;
			this.second = second;
		}
		S first;
		T second;
	}

	public static final String hdfsRootPath = "hdfs://localhost:9000";
	public static final String tableName = "Result";
	public static final String columnFamily = "res";
	
	/**
	 * Split all lines of a file with charactor '|'
	 * @param lines
	 * @return
	 */
	public static ArrayList<String[]> splitFileLines(ArrayList<String> lines) {
		ArrayList<String[]> ret = new ArrayList<String[]>();
		for (String line: lines) {
			ret.add(line.split("\\|"));
		}
		return ret;
	}
	
	/**
	 * Build a hash table with one column value linked with file index.
	 * @param partsOfLines
	 * @param index
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String, ArrayList<Integer>> buildHashTable(ArrayList<String[]> partsOfLines, int index) throws IOException {
		HashMap<String, ArrayList<Integer>> hashTable = new HashMap<String, ArrayList<Integer>>();
		for (int line = 0; line < partsOfLines.size(); line++) {
			String[] partsOfLine = partsOfLines.get(line);
			String key = partsOfLine[index];
			//ArrayList<Integer> values = hashTable.getOrDefault(key, new ArrayList<Integer>());
			ArrayList<Integer> values = null;
			if (hashTable.containsKey(key))
				values = hashTable.get(key);
			else
				values = new ArrayList<Integer>();
			values.add(line);
			hashTable.put(key, values);
		}
		return hashTable;
	}
	
	/**
	 * Join two file, and print the answer to hbase.
	 * @param rPath
	 * @param rIndex
	 * @param sPath
	 * @param sIndex
	 * @param resParams
	 * @throws IOException
	 */
	public static void join(String rPath, int rIndex, String sPath, int sIndex, String[] resParams) throws IOException {
		HashMap<String, ArrayList<Pair<Integer, Integer>>> pairsTable = new HashMap<String, ArrayList<Pair<Integer, Integer>>>();
		ArrayList<String[]> datas = new ArrayList<String[]>();
		
		ArrayList<String> rLines = HDFS.readFromHdfs(hdfsRootPath, rPath);
		ArrayList<String[]> rPartsOfLines = splitFileLines(rLines);
		HashMap<String, ArrayList<Integer>> hashTable = buildHashTable(rPartsOfLines, rIndex);

		ArrayList<String> sLines = HDFS.readFromHdfs(hdfsRootPath, sPath);
		ArrayList<String[]> sPartsOfLines = splitFileLines(sLines);
		for (int line = 0; line < sPartsOfLines.size(); line++) {
			String[] sPartsOfLine = sPartsOfLines.get(line);
			String key = sPartsOfLine[sIndex];
			if (hashTable.containsKey(key)) {
				//ArrayList<Pair<Integer, Integer>> pairs = pairsTable.getOrDefault(key, new ArrayList<Pair<Integer, Integer>>());
				ArrayList<Pair<Integer, Integer>> pairs = null;
				if (pairsTable.containsKey(key))
					pairs = new ArrayList<Pair<Integer, Integer>>();
				else	
					pairs = pairsTable.get(key);
				for (int rLineNumber: hashTable.get(key)) {
					pairs.add(new Pair<Integer, Integer>(rLineNumber, line));
				}
				pairsTable.put(key, pairs);
			}
		}
		
		for (Entry<String, ArrayList<Pair<Integer, Integer>>> entry: pairsTable.entrySet()) {
			String key = entry.getKey();
			ArrayList<Pair<Integer, Integer>> pairs = entry.getValue();
			for (int i = 0; i < pairs.size(); i++) {
				Pair<Integer, Integer> pair = pairs.get(i);
				int rLine = pair.first;
				int sLine = pair.second;
				String suffix = (i == 0 ? "" : "." + i);
				for (String resParam: resParams) {
					int resIndex = Integer.parseInt(resParam.charAt(1) + "");
					String[] args = new String[4];
					args[0] = key;
					args[1] = columnFamily;
					args[2] = resParam + suffix;
					args[3] = (resParam.charAt(0) == 'R' ? rPartsOfLines.get(rLine)[resIndex] : sPartsOfLines.get(sLine)[resIndex]);
					datas.add(args);
				}
			}
		}
		
		HBase.dropTable(tableName);
		HBase.createTable(tableName, columnFamily);
		HBase.put(tableName, datas);
	}
	
	/**
	 * Solve the input arguments and join two files.
	 * @param args
	 * @throws IOException
	 */
	public static void parstInput(String[] args) throws IOException {
		//R=/hw1/lineitem.tbl
		String rPath = args[0].substring(2);
		//S=/hw1/orders.tbl
		String sPath = args[1].substring(2);
		//join:R0=S0
		int rIndex = Integer.parseInt(args[2].substring(6, 7));
		int sIndex = Integer.parseInt(args[2].substring(9, 10));
		//res:S1,S3,R5
		String[] params = args[3].substring(4).split(",");
		join(rPath, rIndex, sPath, sIndex, params);
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		parstInput(args);
	}
}
