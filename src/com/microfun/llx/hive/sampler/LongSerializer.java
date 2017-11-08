package com.microfun.llx.hive.sampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LongSerializer implements Serializer {

	@Override
	public void serialize(ArrayList<Byte> buffer, Object value) {
		assert(value instanceof Long);
		long v = ((Long)value).longValue();
		buffer.add((byte) (v >> 56 ^ 0x80));
		buffer.add((byte) (v >> 48));
		buffer.add((byte) (v >> 40));
		buffer.add((byte) (v >> 32));
		buffer.add((byte) (v >> 24));
		buffer.add((byte) (v >> 16));
		buffer.add((byte) (v >> 8));
		buffer.add((byte) v);
	}

	@Override
	public Object getColumn(ResultSet res, int columnindex) throws SQLException {
		return res.getLong(columnindex);
	}

}
