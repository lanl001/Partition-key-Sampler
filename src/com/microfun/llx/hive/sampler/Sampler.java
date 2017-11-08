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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;

public class Sampler extends Configured implements Tool {
	private static int numreducers;
	private static String sql;
	private ArrayList<ArrayList<Byte>> sampled = new ArrayList<ArrayList<Byte>>();
	private static String driverName;
	private static String url;
	private static String user;
	private static String password;
	private static ResultSet res;
	private static Configuration conf;
	private static Path partFile;
	private static String[] cols;
	private static final Logger log = Logger.getLogger(Sampler.class);

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		conf = new Configuration();
		if (args.length != 1) {
			System.out.println("ERROR: Ivalid argument:\n usage: Sampler config.xml");
		}
		conf.addResource(new Path(args[0]));
		conf.addResource(
				new Path(conf.get("hadoop.core.site.xml", "/opt/hadoop/hadoop-current/etc/hadoop/core-site.xml")));
		conf.addResource(
				new Path(conf.get("hdfs.core.site.xml", "/opt/hadoop/hadoop-current/etc/hadoop/hdfs-site.xml")));
		driverName = conf.get("jdbc.driverName", "org.apache.hive.jdbc.HiveDriver");
		url = conf.get("jdbc.url", "jdbc:hive2://hive-server1:10000/default");
		user = conf.get("jdbc.username");
		if (user == null) {
			System.out.println("ERROR: jdbc.user was not setted!");
			return;
		}
		password = conf.get("jdbc.password");

		cols = conf.getStrings("sample.columns");
		if (cols == null) {
			System.out.println("ERROR: sample.column.name was not setted!");
			return;
		}
		String colstr = new String();
		for (String s : cols) {
			colstr += s;
			colstr += ", ";
		}
		colstr = colstr.substring(0, colstr.lastIndexOf(','));
		String table = conf.get("sample.table");
		if (table == null) {
			System.out.println("ERROR: sample.table.name was not setted!");
			return;
		}
		Integer part = conf.getInt("sample.buckets", 100);
		Integer numrows = conf.getInt("sample.num.rows", 1000);
		sql = "SELECT " + colstr + " FROM\n" + "(SELECT " + colstr + " FROM `" + table
				+ "` tablesample(BUCKET 1 OUT OF " + part.toString() + " ON rand()) s1) s2\n" + "order by rand() limit "
				+ numrows.toString();
		partFile = new Path(conf.get("sample.path", "/tmp/sample.lst"));
		numreducers = conf.getInt("sample.reducer.number", -1);
		if (numreducers == -1) {
			System.out.println("ERROR: sample.reducer.number was not setted!");
			return;
		}
		ToolRunner.run(new Sampler(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Log.info(conf.getRaw("fs.defaultFS"));
		getTableSample(cols.length);
		byte[][] partitionkeys = getPartitionKeys(numreducers);
		HiveKey key = new HiveKey();
		NullWritable value = NullWritable.get();
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, partFile, HiveKey.class, NullWritable.class);
		for (byte[] pkey : partitionkeys) {
			key.set(new BytesWritable(pkey));
			writer.append(key, value);
		}
		writer.close();
		System.out.println("Sampling has done!");
		return 0;
	}

	private void getTableSample(int numcolumns) {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();
			res = stmt.executeQuery(sql);
			while (res.next()) {
				ArrayList<Byte> line = new ArrayList<Byte>();
				Serializer ser;
				for (int i = 1; i <= numcolumns; ++i) {
					int t = res.getMetaData().getColumnType(i);
					if (t == Types.BIGINT) {
						ser = new LongSerializer();

					} else if (t == Types.INTEGER) {
						ser = new IntSerializer();
					} else {
						System.out.println("ERROR: Unknown column type!");
						return;
					}
					line.addAll(toHiveKey(ser, i));
				}
				sampled.add(line);
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
			} catch (SQLException e) {
				// e.printStackTrace();
			}
		}
	}

	private Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}


	public static final Comparator<byte[]> C = new Comparator<byte[]>() {
		public final int compare(byte[] o1, byte[] o2) {
			return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
		}
	};

	private byte[][] getPartitionKeys(int numReduce) {
		if (sampled.size() < numReduce - 1) {
			throw new IllegalStateException("not enough number of sample");
		}
		byte[][] sorted = new byte[sampled.size()][];
		for (int i = 0; i < sampled.size(); ++i) {
			ArrayList<Byte> line = sampled.get(i);
			sorted[i] = new byte[line.size()];
			for (int j = 0; j < line.size(); ++j) {
				sorted[i][j] = line.get(j).byteValue();
			}
		}
		Arrays.sort(sorted, C);

		return toPartitionKeys(sorted, numReduce);
	}

	static final byte[][] toPartitionKeys(byte[][] sorted, int numPartition) {
		byte[][] partitionKeys = new byte[numPartition - 1][];
		int last = 0;
		int current = 0;
		for (int i = 0; i < numPartition - 1; i++) {
			current += Math.round((float) (sorted.length - current) / (numPartition - i));
			while (i > 0 && current < sorted.length && C.compare(sorted[last], sorted[current]) == 0) {
				current++;
			}
			if (current >= sorted.length) {
				return Arrays.copyOfRange(partitionKeys, 0, i);
			}
			if (log.isDebugEnabled()) {
				// print out nth partition key for debugging
				log.debug("Partition key " + current + "th :" + new BytesWritable(sorted[current]));
			}
			partitionKeys[i] = sorted[current];
			last = current;
		}
		return partitionKeys;
	}

	private ArrayList<Byte> toHiveKey(Serializer s, int column) throws SQLException {
		Object o = s.getColumn(res, column);
		ArrayList<Byte> buffer = new ArrayList<Byte>();
		if (o == null) {
			buffer.add((byte) 0x0);
		}
		buffer.add((byte) 0x1);
		s.serialize(buffer, o);
		return buffer;
	}
}