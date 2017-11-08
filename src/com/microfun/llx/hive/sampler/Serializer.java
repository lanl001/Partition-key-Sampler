package com.microfun.llx.hive.sampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public interface Serializer {
	public void serialize(ArrayList<Byte> buffer, Object value);
	public Object getColumn(ResultSet res, int columnindex) throws SQLException;
}
