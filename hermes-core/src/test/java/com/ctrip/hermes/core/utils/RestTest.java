package com.ctrip.hermes.core.utils;

public class RestTest {
	//
	// private HttpClient m_httpClient;
	//
	// private RequestConfig m_requestConfig = RequestConfig.DEFAULT;
	//
	// @Test
	// public void test() throws Exception {
	// m_httpClient = HttpClients.createDefault();
	//
	// Builder b = RequestConfig.custom();
	// // TODO config
	// b.setConnectTimeout(2000);
	// b.setSocketTimeout(2000);
	// m_requestConfig = b.build();
	//
	// String url = String.format("http://%s%s", "127.0.0.1:1248", "/lease/acquire");
	// HttpPost post = new HttpPost(url);
	// post.setConfig(m_requestConfig);
	//
	// HttpResponse response;
	// post.setEntity(new StringEntity(JSON.toJSONString(new Tpg("topic", 1, "1")), ContentType.APPLICATION_JSON));
	// response = m_httpClient.execute(post);
	// System.out.println(EntityUtils.toString(response.getEntity()));
	// }

}
