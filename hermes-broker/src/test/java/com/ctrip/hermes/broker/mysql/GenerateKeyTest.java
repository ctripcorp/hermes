package com.ctrip.hermes.broker.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;

import com.mysql.jdbc.Statement;

public class GenerateKeyTest {

	@Test
	public void test() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1", "root", null);

		PreparedStatement stmt = conn.prepareStatement("insert into test.test (name) values (?)",
		      Statement.RETURN_GENERATED_KEYS);

		stmt.setString(1, "a");
		stmt.addBatch();
		stmt.setString(1, "b");
		stmt.addBatch();

		stmt.executeBatch();

		ResultSet rs = stmt.getGeneratedKeys();
		while (rs.next()) {
			System.out.println(rs.getLong(1));
		}
	}

}
