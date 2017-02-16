package decoder;

import java.io.File;

import org.unidal.helper.Files.IO;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.message.payload.GZipPayloadCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodecCompositor;
import com.ctrip.hermes.core.message.payload.RawMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * 
 */

/**
 * @author marsqing
 *
 *         Jul 15, 2016 5:32:49 PM
 */
public class JsonGzipDecoder {

	public static void main(String[] args) throws Exception {
		byte[] dat = IO.INSTANCE.readFrom(new File("/Users/marsqing/Downloads/forwin/kafka.dat"));

		ByteBuf buf = Unpooled.wrappedBuffer(dat);
		BaseConsumerMessage<?> dd = new DefaultMessageCodec().decode("", buf, RawMessage.class);

		RawMessage raw = (RawMessage) dd.getBody();

		PayloadCodecCompositor cc = new PayloadCodecCompositor("json,gzip");

		cc.addPayloadCodec(new JsonPayloadCodec());
		cc.addPayloadCodec(new GZipPayloadCodec());

		String xx = cc.decode(raw.getEncodedMessage(), String.class);

		System.out.println(xx);
	}

}
