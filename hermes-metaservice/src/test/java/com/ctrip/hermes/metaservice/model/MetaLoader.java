package com.ctrip.hermes.metaservice.model;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.google.common.io.Files;

public class MetaLoader extends ComponentTestCase {

	@Test
	public void loadMeta() throws DalException, IOException {
		MetaService metaService = lookup(MetaService.class);
		com.ctrip.hermes.meta.entity.Meta metaDB = metaService.getMetaEntity();
		System.out.println(metaDB);

		String text = Files.toString(new File("src/test/resources/meta-dev.json"), Charset.forName("UTF-8"));
		com.ctrip.hermes.meta.entity.Meta metaFile = JSON.parseObject(text, com.ctrip.hermes.meta.entity.Meta.class);
		System.out.println(metaFile);

		Assert.assertEquals(metaFile.getApps(), metaDB.getApps());
		Assert.assertEquals(metaFile.getCodecs(), metaDB.getCodecs());
		Assert.assertEquals(metaFile.getEndpoints(), metaDB.getEndpoints());
		Assert.assertEquals(metaFile.getServers(), metaDB.getServers());
		Assert.assertEquals(metaFile.getStorages(), metaDB.getStorages());
		Assert.assertEquals(metaFile.getTopics(), metaDB.getTopics());
		Assert.assertEquals(metaFile.getVersion(), metaDB.getVersion());
		Assert.assertEquals(metaFile.getId(), metaDB.getId());
		Assert.assertEquals(metaFile, metaDB);
	}

}
