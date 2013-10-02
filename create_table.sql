CREATE TABLE `dalga` (
  `routing_key`	VARCHAR(255)	NOT NULL,
  `body`	 	VARCHAR(255)	NOT NULL,
  `interval` 	INT UNSIGNED	NOT NULL,
  `next_run` 	DATETIME 		NOT NULL,

  PRIMARY KEY (`routing_key`, `body`),
  KEY `idx_next_run` (`next_run`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
