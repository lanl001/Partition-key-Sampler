package com.microfun.llx.hive.sampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class IntSerializer implements Serializer {

	@Override
	public void serialize(ArrayList<Byte> buffer, Object value) {
		assert(value instanceof Integer);
		int v = ((Integer)value).intValue();
		buffer.add((byte) ((v >> 24) ^ 0x80));
		buffer.add((byte) (v >> 16));
		buffer.add((byte) (v >> 8));
		buffer.add((byte) v);
	}

	@Override
	public Object getColumn(ResultSet res, int columnindex) throws SQLException {
		return res.getInt(columnindex);
	}

}
