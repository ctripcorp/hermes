package com.ctrip.hermes.metaservice.zk;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ZKPathUtils {

	private static final String PATH_SEPARATOR = "/";

	private static final String META_SERVER_ASSIGNMENT_PATH_ROOT = "/metaserver-assignment";

	private static final String META_SERVER_ASSIGNMENT_PATH_PATTERN = META_SERVER_ASSIGNMENT_PATH_ROOT + "/%s";

	public static String getBrokerRegistryBasePath() {
		return "brokers";
	}

	public static String getBrokerRegistryName(String name) {
		return "default";
	}

	public static String getCmessageExchangePath() {
		return "/cmessage-exchange";
	}

	public static String getCmessageConfigPath() {
		return "/cmessage-config";
	}

	public static String getBaseMetaVersionZkPath() {
		return "/base-meta-version";
	}

	public static String getMetaInfoZkPath() {
		return "/meta-info";
	}

	public static String getMetaServersZkPath() {
		return "/meta-servers";
	}

	public static String lastSegment(String path) {
		int lastSlashIdx = path.lastIndexOf(PATH_SEPARATOR);

		if (lastSlashIdx >= 0) {
			return path.substring(lastSlashIdx + 1);
		} else {
			return path;
		}
	}

	public static String getMetaServerAssignmentRootZkPath() {
		return META_SERVER_ASSIGNMENT_PATH_ROOT;
	}

	public static String getMetaServerAssignmentZkPath(String topic) {
		return String.format(META_SERVER_ASSIGNMENT_PATH_PATTERN, topic);
	}

}
