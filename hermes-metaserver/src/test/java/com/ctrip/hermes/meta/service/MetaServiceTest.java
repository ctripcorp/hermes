package com.ctrip.hermes.meta.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.unidal.dal.jdbc.DalException;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.model.Meta;
import com.ctrip.hermes.metaservice.model.MetaDao;

public class MetaServiceTest {

	public static void main(String[] args) throws FileNotFoundException {
		MetaDao metaDao = PlexusComponentLocator.lookup(MetaDao.class);
		File file = new File("src/test/resources/meta-local.xml");
		InputStream in = new FileInputStream(file);
		try {
			com.ctrip.hermes.meta.entity.Meta meta = DefaultSaxParser.parse(in);
			Meta dalMeta = new Meta();
			dalMeta.setValue(JSON.toJSONString(meta));
			metaDao.insert(dalMeta);
		} catch (SAXException | IOException | DalException e) {
			e.printStackTrace();
		}
	}
}
