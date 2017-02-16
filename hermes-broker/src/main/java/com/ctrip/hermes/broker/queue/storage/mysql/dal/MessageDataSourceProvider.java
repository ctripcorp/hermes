package com.ctrip.hermes.broker.queue.storage.mysql.dal;

import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;

@Named(type = DataSourceProvider.class, value = "message")
public class MessageDataSourceProvider implements DataSourceProvider {

	@Inject
	private MetaService m_metaService;

	@Override
	public DataSourcesDef defineDatasources() {
		DataSourcesDef def = new DataSourcesDef();

		List<Datasource> dataSources = m_metaService.listAllMysqlDataSources();
		for (Datasource ds : dataSources) {
			Map<String, Property> dsProps = ds.getProperties();
			DataSourceDef dsDef = new DataSourceDef(ds.getId());
			PropertiesDef props = new PropertiesDef();

			props.setDriver("com.mysql.jdbc.Driver");
			if (dsProps.get("url") == null || dsProps.get("user") == null) {
				throw new IllegalArgumentException("url and user property can not be null in datasource definition " + ds);
			}
			props.setUrl(dsProps.get("url").getValue());
			props.setUser(dsProps.get("user").getValue());
			if (dsProps.get("password") != null) {
				props.setPassword(dsProps.get("password").getValue());
			}

			props.setConnectionProperties("useUnicode=true&autoReconnect=true&rewriteBatchedStatements=true");
			dsDef.setProperties(props);

			int maxPoolSize = 20;
			if (dsProps.get("maximumSize") != null) {
				maxPoolSize = Integer.parseInt(dsProps.get("maximumSize").getValue());
			}
			dsDef.setMaximumPoolSize(maxPoolSize);

			int minPoolSize = 10;
			if (dsProps.get("minimumSize") != null) {
				minPoolSize = Integer.parseInt(dsProps.get("minimumSize").getValue());
			}
			dsDef.setMinimumPoolSize(minPoolSize);

			int checkoutTimeoutMillis = 1000;
			if (dsProps.get("checkoutTimeoutMillis") != null) {
				checkoutTimeoutMillis = Integer.parseInt(dsProps.get("checkoutTimeoutMillis").getValue());
			}
			dsDef.setCheckoutTimeoutInMillis(checkoutTimeoutMillis);

			def.addDataSource(dsDef);
		}

		return def;
	}
}
