USE `hermes`;

CREATE TABLE IF NOT EXISTS `meta_token` (
  `id` int(11) NOT NULL,
  `token` int(11) NOT NULL,
  `type` enum('p','c') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `token` (`token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='producer_token & consumer_token';

CREATE TABLE IF NOT EXISTS `msg_0_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` blob NOT NULL,
  `source_ip` int(11),
  `token` int(11),
  `key` varchar(50) DEFAULT NULL,
  `properties` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Topic: testtopic\r\nPriority: high.';

CREATE TABLE IF NOT EXISTS `msg_1_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` blob NOT NULL,
  `source_ip` int(11) NOT NULL,
  `token` int(11) NOT NULL,
  `key` varchar(50) DEFAULT NULL,
  `properties` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Topic: TestTopic\r\nPriority: log.';

CREATE TABLE IF NOT EXISTS `msg_offset_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) NOT NULL,
  `target` varchar(50) NOT NULL,
  `offset` int(11) NOT NULL COMMENT 'refer to msg_p_[topic]_[priority]:id',
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id and target` (`group_id`, `target`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='consume offset.';

CREATE TABLE IF NOT EXISTS `resend_${topic}_group1` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start` int(11) NOT NULL,
  `end` int(11) NOT NULL,
  `priority` TINYINT(4) NOT NULL,
  `due` timestamp NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='resend high priority messages.';

CREATE TABLE IF NOT EXISTS `resend_offset_${topic}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(50) NOT NULL,
  `target` varchar(50) NOT NULL,
  `offset` int(11) NOT NULL COMMENT 'refer to msg_p_[topic]_[priority]:id',
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='consume offset.';
