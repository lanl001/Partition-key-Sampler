package com.microfun.llx.hive.sampler;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import jline.internal.Log;

import org.apache.hadoop.util.Tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;

public class Sampler extends Configured implements Tool {
	private static int numreducers;
	private static String sql;
	private List<Object> sampled = new ArrayList<Object>();
	private static String driverName;
	private static String url;
	private static String user;
	private static String password;
	private static ResultSet res;
	private static Configuration conf;
	private static Path partFile;
	private static final Logger log = Logger.getLogger(Sampler.class);

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		conf = new Configuration();
		if(args.length != 1) {
			System.out.println("ERROR: Ivalid argument:\n usage: Sampler config.xml");
		}
		conf.addResource(new Path(args[0]));
		conf.addResource(new Path(conf.get("hadoop.core.site.xml", "/opt/hadoop/hadoop-current/etc/hadoop/core-site.xml")));
		conf.addResource(new Path(conf.get("hdfs.core.site.xml", "/opt/hadoop/hadoop-current/etc/hadoop/hdfs-site.xml")));
		driverName = conf.get("jdbc.driverName","org.apache.hive.jdbc.HiveDriver");
		url = conf.get("jdbc.url","jdbc:hive2://hive-server1:10000/default");
		user = conf.get("jdbc.username");
		if(user == null) {
			System.out.println("ERROR: jdbc.user was not setted!");
			return;
		}
		password = conf.get("jdbc.password");
		
		String col = conf.get("sample.column.name");
		if(col == null) {
			System.out.println("ERROR: sample.column.name was not setted!");
			return;
		}
		String table = conf.get("sample.table.name");
		if(table == null) {
			System.out.println("ERROR: sample.table.name was not setted!");
			return;
		}
		Integer part = conf.getInt("sample.buckets",100);
		Integer numrows = conf.getInt("sample.num.rows", 1000);
		sql = "SELECT "+ col + " FROM\n"
				+ "(SELECT " + col + " FROM `" + table + "` tablesample(BUCKET 1 OUT OF " + part.toString() + " ON rand()) s1) s2\n"
				+ "order by rand() limit " + numrows.toString();
		partFile = new Path(conf.get("sample.path", "/tmp/sample.lst"));
		numreducers = conf.getInt("sample.reducer.number",-1);
		if(numreducers == -1) {
			System.out.println("ERROR: sample.reducer.number was not setted!");
			return;
		}
		ToolRunner.run(new Sampler(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Log.info(conf.getRaw("fs.defaultFS"));

		getTableSample();
		Object[] partitionkeys = toPartitionKeys(numreducers);
		HiveKey key = new HiveKey();
		NullWritable value = NullWritable.get();
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, partFile, HiveKey.class, NullWritable.class);
		for(Object pkey:partitionkeys) {
			key.set(toHiveKey(pkey));
			writer.append(key, value);
		}
		writer.close();
		System.out.println("Sampling has done!");
		return 0;
	}

	private void getTableSample() {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();
			res = stmt.executeQuery(sql);
			int t = res.getMetaData().getColumnType(1);
			if(t == Types.BIGINT) {
				while (res.next()) {
						sampled.add(res.getLong(1));
		        }
			}
			else if(t == Types.INTEGER) {
				while (res.next()) {
						sampled.add(res.getInt(1));
		        }
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error(driverName + " not found!", e);
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("Connection error!", e);
			System.exit(1);
		} finally {
			try {
				if (conn != null) {
					conn.close();
					conn = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
			}catch (SQLException e) {
				//e.printStackTrace();
			}
		}
	}

	private Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

	private Object[] toPartitionKeys(int numReduce) {
		if (sampled.size() < numReduce - 1) {
			throw new IllegalStateException("not enough number of sample");
		}
		Object[] sorted = sampled.toArray(new Object[numReduce - 1]);
		Arrays.sort(sorted);
		Object[] partitionKeys = new Object[numReduce - 1];
		float stepSize = sampled.size() / (float) numReduce;
		int last = -1;
		for (int i = 1; i < numReduce; ++i) {
			int k = Math.round(stepSize * i);
			while (last >= k && sorted[last].equals(sorted[k])) {
				k++;
			}
			partitionKeys[i - 1] = sorted[k];
			last = k;
		}
		return partitionKeys;
	}
	
	private BytesWritable toHiveKey(Object o) {
		ArrayList<Byte> buffer = new ArrayList<Byte>();
		if (o == null) {
		      return new BytesWritable(new byte[] {0});
		}
		buffer.add((byte)0x1);
		Class<?> clazz = o.getClass();
		if(clazz == Long.class) {
			serializeLong(buffer,((Long)o).longValue());
		}
		else if(clazz == Integer.class) {
			serializeLong(buffer,((Integer)o).intValue());
		}
		else {
			log.error("Unexpected type for hivekey!");
			System.exit(1);
		}
		byte[] ret = new byte[buffer.size()];
		for(int i = 0; i < buffer.size(); ++i) {
			ret[i] = buffer.get(i).byteValue();
		}
		return new BytesWritable(ret);	
	}

	public static void serializeInt(ArrayList<Byte> buffer, int v) {
	    buffer.add((byte) ((v >> 24) ^ 0x80));
	    buffer.add((byte) (v >> 16));
	    buffer.add((byte) (v >> 8));
	    buffer.add((byte) v);
	}
	
	public static void serializeLong(ArrayList<Byte> buffer, long v){
		buffer.add((byte) (v >> 56 ^ 0x80));
		buffer.add((byte) (v >> 48));
		buffer.add((byte) (v >> 40));
		buffer.add((byte) (v >> 32));
		buffer.add((byte) (v >> 24));
	    buffer.add((byte) (v >> 16));
	    buffer.add((byte) (v >> 8));
	    buffer.add((byte) v);
	}
}