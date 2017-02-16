package com.ctrip.hermes.core;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.Before;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class HermesCoreBaseTest extends ComponentTestCase {
	@Before
	public void setUp() throws Exception {
		super.setUp();
		defineComponent(MetaService.class, TestMetaService.class);
	}

	protected Map<String, String> readProperties(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		return codec.readStringStringMap();
	}

	protected void writeProperties(List<Pair<String, String>> properties, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		if (properties != null) {
			Map<String, String> map = new HashMap<String, String>();
			for (Pair<String, String> prop : properties) {
				map.put(prop.getKey(), prop.getValue());
			}

			codec.writeStringStringMap(map);
		} else {
			codec.writeNull();
		}
	}

	public static class TestMetaService extends DefaultMetaService {

		@Override
		public Topic findTopicByName(String topicName) {
			Topic topic = new Topic();
			topic.setCodecType(Codec.JSON);
			return topic;
		}

		@Override
		public void initialize() throws InitializationException {
			// do nothing
		}

	}
}
