package setup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.junit.Test;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.dal.jdbc.datasource.model.transform.DefaultSaxParser;
import org.unidal.helper.Files.IO;

public class Setup {

	/*
	 * 1. create db `meta` 
	 * 2. create db `FxHermesShard01DB` 
	 * 3. eclipse user: install "Remote Launch" in http://download.eclipse.org/tools/cdt/releases/8.6
	 */

	@Test
	public void test() throws Exception {
		Connection conn = createMetaDBConnection();

		copyDatasourcesXml();
		createMetaDBTables(conn);
		insertInitMetaJson(conn);
	}

	private void insertInitMetaJson(Connection conn) throws Exception {
		String sql = "INSERT INTO meta (value, DataChange_LastTime) VALUES (?, ?)";
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setBlob(1, Setup.class.getResourceAsStream("meta.json"));
		stmt.setDate(2, new Date(new java.util.Date().getTime()));
		stmt.executeUpdate();
	}

	private void createMetaDBTables(Connection conn) throws Exception {
		InputStream metaDbDdlIn = Setup.class.getResourceAsStream("meta_db_ddl.sql");
		SQLRunner runner = new SQLRunner(conn, false, true);
		runner.runScript(new InputStreamReader(metaDbDdlIn));
	}

	private Connection createMetaDBConnection() throws Exception {
		InputStream dsIn = Setup.class.getResourceAsStream("datasources.xml");
		DataSourcesDef def = DefaultSaxParser.parse(dsIn);
		PropertiesDef ps = def.getDataSourcesMap().get("fxhermesmetadb").getProperties();
		Class.forName(ps.getDriver());
		Connection conn = DriverManager.getConnection(ps.getUrl(), ps.getUser(), ps.getPassword());
		return conn;
	}

	private void copyDatasourcesXml() throws IOException, FileNotFoundException {
		InputStream dsIn = Setup.class.getResourceAsStream("datasources.xml");
		File dsOut = new File("/data/appdatas/hermes/datasources.xml");
		dsOut.getParentFile().mkdirs();
		IO.INSTANCE.copy(dsIn, new FileOutputStream(dsOut));
	}

}
