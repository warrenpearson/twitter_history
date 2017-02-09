#!/usr/bin/env ruby

require 'twitter'
require 'mysql2'
require './conf'

# Class to retrieve all tweets for all users
# specified in an input file, and store them
# in a MySQL database.
class StoreHistoricalTweets
  def initialize
    @client = twitter_client
    @db = db_handle
    @twitter_calls = 0
  end

  def twitter_client
    Twitter::REST::Client.new do |config|
      config.consumer_key        = CONSUMER_KEY
      config.consumer_secret     = CONSUMER_SECRET
      config.access_token        = ACCESS_TOKEN
      config.access_token_secret = ACCESS_TOKEN_SECRET
    end
  end

  def db_handle
    Mysql2::Client.new(
      host: DB_HOST, username: DB_USER,
      password: DB_PASS, database: DATABASE,
      encoding: 'utf8mb4', reconnect: true
    )
  end

  def increment_twitter_api_calls
    @twitter_calls += 1
  end

  def tweets_for_users(input_file)
    users = users_from_file(input_file)
    users.each do |user|
      store_tweets_for_user(user)
      exit if @twitter_calls > 150
    end
  end

  def users_from_file(input_file)
    File.readlines(input_file)
  end

  def store_tweets_for_user(user)
    puts "Storing tweets for #{user}, API call count = #{@twitter_calls}"
    tweets = get_tweets_for_user(user)
    tweets.each do |tweet|
      store_in_db(user, tweet)
    end
  end

  def get_tweets_for_user(user)
    user.chomp!
    sql_max = get_max_id(user)
    if sql_max
      puts "Rows exists for #{user}, nothing to be done"
      return []
    end
    collect_with_max_id do |max_id|
      puts "max_id: #{max_id}"
      increment_twitter_api_calls
      options = { count: 200, include_rts: true }
      options[:max_id] = max_id unless max_id.nil?
      @client.user_timeline(user, options)
    end
  end

  def get_max_id(user)
    max_id = nil
    handle = @db.escape(user)
    qry = "SELECT max(tweet_id) as max_id from tweet where twitter_handle = '#{handle}'"
    rows = @db.query(qry)

    puts rows.first['max_id']
    rows.first['max_id']
  end

  def collect_with_max_id(collection = [], max_id = nil, &block)
    response = yield(max_id)
    collection += response

    return collection.flatten if response.empty?

    collect_with_max_id(collection, response.last.id - 1, &block)
  end

  def store_in_db(user, tweet)
    handle = @db.escape(user)

    tweet_text = @db.escape(tweet.text)
    qry  = 'INSERT INTO tweet VALUES('
    qry += "'#{tweet.id}', '#{tweet_text}', '#{handle}', "
    qry += "'#{tweet.created_at}', now())"
    puts qry
    @db.query(qry)

    # TODO: check status code and retry
    # TODO: timestamps on logs
  end
end

input_file = ARGV[0]

unless input_file
  puts 'Please specify an input file of twitter handles'
  exit
end

StoreHistoricalTweets.new.tweets_for_users(input_file)
