package com.ctrip.hermes.core.meta.internal;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.xml.sax.SAXException;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

/**
 * for test only
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaLoader.class, value = LocalMetaLoader.ID)
public class LocalMetaLoader implements MetaLoader {

	private static final Logger log = LoggerFactory.getLogger(LocalMetaLoader.class);

	public static final String ID = "local-meta-loader";

	private static final String PATH = "/com/ctrip/hermes/meta/meta-local.xml";

	@Override
	public Meta load() {
		if (log.isDebugEnabled()) {
			log.debug("Loading meta from local: {}", PATH);
		}
		InputStream in = getClass().getResourceAsStream(PATH);

		if (in == null) {
			throw new RuntimeException(String.format("Local meta file %s not found on classpath", PATH));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (SAXException e) {
				throw new RuntimeException(String.format("Error parse local meta file %s", PATH), e);
			} catch (IOException e) {
				throw new RuntimeException(String.format("Error parse local meta file %s", PATH), e);
			}
		}
	}
	// public static void main(String[] args) throws IOException, SAXException {
	// outputMetaStringFromMeta_Local();
	//
	//
	// // put meta XML here:
	// String metaXML = "";
	// // outputMetaStringByMetaXML(metaXML);
	//
	//
	// // put meta String (mostly from db) here:
	// String metaString = "";
	// // outputMetaXMLFromMetaString(metaString);
	// }
	//
	// private static void outputMetaStringFromMeta_Local() {
	// LocalMetaLoader loader = new LocalMetaLoader();
	// System.out.println(JSON.toJSONString(loader.load()));
	// }
	//
	// private static void outputMetaXMLFromMetaString(String metaString) {
	// Meta meta = JSON.parseObject(metaString,
	// Meta.class);
	// System.out.println(meta);
	// }
	//
	// private static void outputMetaStringByMetaXML(String metaXML) throws SAXException, IOException {
	// Meta meta = DefaultSaxParser.parse(metaXML);
	// System.out.println(JSON.toJSONString(meta));
	// }
}
