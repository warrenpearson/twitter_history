CREATE database IF NOT EXISTS `historical_scoring`;

use `historical_scoring`;

CREATE table IF NOT EXISTS tweet (
tweet_id varchar(20),
tweet_text varchar(500),
twitter_handle varchar(50),
tweet_time varchar(50),
inserted_at datetime
);
