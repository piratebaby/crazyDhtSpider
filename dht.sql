SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- 数据库： `dht`
--

-- --------------------------------------------------------

--
-- 表的结构 `bt`
--

CREATE TABLE `bt` (
  `name` varchar(500) NOT NULL COMMENT '名称',
  `keywords` varchar(250) DEFAULT NULL COMMENT '关键词',
  `length` bigint(20) NOT NULL DEFAULT '0' COMMENT '文件大小',
  `piece_length` int(11) NOT NULL DEFAULT '0' COMMENT '种子大小',
  `infohash` char(40) NOT NULL COMMENT '种子哈希值',
  `files` json DEFAULT NULL COMMENT '文件列表',
  `hits` int(11) NOT NULL DEFAULT '0' COMMENT '点击量',
  `hot` int(11) NOT NULL DEFAULT '1' COMMENT '热度',
  `time` datetime NOT NULL COMMENT '收录时间',
  `lasttime` datetime NOT NULL COMMENT '最后下载时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED
PARTITION BY KEY (infohash)
(
PARTITION p0 ENGINE=InnoDB,
PARTITION p1 ENGINE=InnoDB,
PARTITION p2 ENGINE=InnoDB,
PARTITION p3 ENGINE=InnoDB,
PARTITION p4 ENGINE=InnoDB,
PARTITION p5 ENGINE=InnoDB,
PARTITION p6 ENGINE=InnoDB,
PARTITION p7 ENGINE=InnoDB,
PARTITION p8 ENGINE=InnoDB,
PARTITION p9 ENGINE=InnoDB,
PARTITION p10 ENGINE=InnoDB,
PARTITION p11 ENGINE=InnoDB,
PARTITION p12 ENGINE=InnoDB,
PARTITION p13 ENGINE=InnoDB,
PARTITION p14 ENGINE=InnoDB,
PARTITION p15 ENGINE=InnoDB,
PARTITION p16 ENGINE=InnoDB,
PARTITION p17 ENGINE=InnoDB,
PARTITION p18 ENGINE=InnoDB,
PARTITION p19 ENGINE=InnoDB,
PARTITION p20 ENGINE=InnoDB,
PARTITION p21 ENGINE=InnoDB,
PARTITION p22 ENGINE=InnoDB,
PARTITION p23 ENGINE=InnoDB,
PARTITION p24 ENGINE=InnoDB,
PARTITION p25 ENGINE=InnoDB,
PARTITION p26 ENGINE=InnoDB,
PARTITION p27 ENGINE=InnoDB,
PARTITION p28 ENGINE=InnoDB,
PARTITION p29 ENGINE=InnoDB,
PARTITION p30 ENGINE=InnoDB
);

-- --------------------------------------------------------

--
-- 表的结构 `history`
--

CREATE TABLE `history` (
  `infohash` char(40) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;

-- --------------------------------------------------------

--
-- 表的结构 `torrent_indices`
--

CREATE TABLE `torrent_indices` (
  `infohash` char(40) NOT NULL COMMENT '存储 40 位 HEX 字符串',
  `file_category` varchar(20) NOT NULL DEFAULT 'Other' COMMENT '文件大类',
  `primary_ext` varchar(64) DEFAULT NULL,
  `total_size` bigint(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT '总大小',
  `file_count` int(10) UNSIGNED NOT NULL DEFAULT '1' COMMENT '文件数量',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转储表的索引
--

--
-- 表的索引 `bt`
--
ALTER TABLE `bt`
  ADD UNIQUE KEY `infohash` (`infohash`) USING BTREE,
  ADD KEY `hot` (`hot`),
  ADD KEY `idx_lasttime` (`lasttime`);

--
-- 表的索引 `history`
--
ALTER TABLE `history`
  ADD PRIMARY KEY (`infohash`),
  ADD UNIQUE KEY `infohash` (`infohash`) USING BTREE;

--
-- 表的索引 `torrent_indices`
--
ALTER TABLE `torrent_indices`
  ADD PRIMARY KEY (`infohash`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
