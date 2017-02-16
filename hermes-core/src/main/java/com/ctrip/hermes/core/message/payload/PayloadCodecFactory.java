package com.ctrip.hermes.core.message.payload;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PayloadCodecFactory {

	private static ConcurrentMap<String, PayloadCodecCompositor> compositorCache = new ConcurrentHashMap<>();

	public static PayloadCodec getCodecByTopicName(String topicName) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		Topic topic = metaService.findTopicByName(topicName);
		return getCodecByType(topic.getCodecType());
	}

	public static PayloadCodec getCodecByType(String codecString) {
		CodecDesc codecDesc = CodecDesc.valueOf(codecString);

		if (codecDesc == null) {
			throw new IllegalArgumentException(String.format("codec type '%s' is illegal", codecString));
		} else if (codecDesc.getCompressionAlgo() == null) {
			return PlexusComponentLocator.lookup(PayloadCodec.class, codecString);
		} else {
			PayloadCodecCompositor cached = compositorCache.get(codecString);

			if (cached != null) {
				return cached;
			} else {
				synchronized (PayloadCodecFactory.class) {
					cached = compositorCache.get(codecString);
					if (cached == null) {
						cached = new PayloadCodecCompositor(codecString);
						cached.addPayloadCodec(PlexusComponentLocator.lookup(PayloadCodec.class, codecDesc.getCodec()));

						if (Codec.GZIP.equals(codecDesc.getCompressionAlgo())) {
							cached.addPayloadCodec(PlexusComponentLocator.lookup(PayloadCodec.class, Codec.GZIP));
						} else if (Codec.DEFLATER.equals(codecDesc.getCompressionAlgo())) {

							int compressionLevel = 5;

							if (codecDesc.getLevel() != -1) {
								compressionLevel = codecDesc.getLevel();
							}
							cached.addPayloadCodec(PlexusComponentLocator.lookup(PayloadCodec.class, Codec.DEFLATER + "-"
							      + compressionLevel));
						}

						compositorCache.put(codecString, cached);
					}
				}

				return cached;
			}
		}

	}

	public static class CodecDesc {
		private String m_codec;

		private String m_compressionAlgo;

		private int m_level;

		public CodecDesc(String codec, String compressionAlgo, int level) {
			m_codec = codec;
			m_compressionAlgo = compressionAlgo;
			m_level = level;
		}

		public String getCodec() {
			return m_codec;
		}

		public String getCompressionAlgo() {
			return m_compressionAlgo;
		}

		public int getLevel() {
			return m_level;
		}

		public static CodecDesc valueOf(String codecString) {
			try {
				String[] parts = codecString.split(",");
				if (parts.length == 1) {
					return new CodecDesc(parts[0].trim(), null, -1);
				} else if (parts.length == 2) {
					String codec = parts[0].trim();
					String compressionAlgo = parts[1].trim();
					if (compressionAlgo.equals(Codec.GZIP)) {
						return new CodecDesc(codec, Codec.GZIP, -1);
					} else if (compressionAlgo.startsWith(Codec.DEFLATER)) {
						String level = compressionAlgo.substring(Codec.DEFLATER.length());
						if (level.startsWith("(") && level.endsWith(")")) {
							return new CodecDesc(codec, Codec.DEFLATER,
							      Integer.valueOf(level.substring(1, level.length() - 1)));
						}

					}
				}
				// fall back to json
				return new CodecDesc(Codec.JSON, null, -1);
			} catch (Exception e) {
				return null;
			}
		}

		@Override
		public String toString() {
			return "CodecDesc [m_codec=" + m_codec + ", m_compressionAlgo=" + m_compressionAlgo + ", m_level=" + m_level
			      + "]";
		}

	}

}
