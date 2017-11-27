require 'aws-sdk'
require 'pry'

class Consumer
  def initialize; end;

  def consume
  	puts 'Exit with Ctrl + C'
  	shard_iterator = get_shard_iterator
  	running = true

		until running == false
			response = kinesis.get_records({
			  shard_iterator: shard_iterator, # required
			  limit: 1
			})

			puts response.records
			shard_iterator = response.next_shard_iterator 
		end
	rescue SystemExit, Interrupt
  	puts "Exiting"
  	exit 130
	end

  private

  def stream_name
    'TestStream'
  end

  def get_shard_iterator
  	kinesis.get_shard_iterator({
			stream_name: stream_name,
			shard_id: "shardId-000000000000", # required
			shard_iterator_type: "TRIM_HORIZON", # required, accepts AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP
			timestamp: Time.now
    }).shard_iterator
  end	

  def credentials
    Aws::SharedCredentials.new(profile_name: 'development')
  end

  def kinesis
    @kinesis ||= Aws::Kinesis::Client.new(region: 'eu-west-1', credentials: credentials)
  end
end