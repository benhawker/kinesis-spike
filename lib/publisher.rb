require 'aws-sdk'

class Publisher
  def initialize; end;

  # Some reading around Kinesis Partition keys and their relationship to a shard.
  # https://stackoverflow.com/questions/31348606/how-to-decide-total-number-of-partition-keys-in-aws-kinesis-stream

  def publish
    create_stream unless stream_exists?

    kinesis.put_record({
      stream_name: stream_name,
      data: "Some data with a random number #{rand(100)}",
      partition_key: "#{rand(100000)}",
    })
  end

  def create_stream
    kinesis.create_stream({
      stream_name: stream_name,
      shard_count: 1,
    })
  end

  private

  def stream_name
    'TestStream'
  end

  def credentials
    Aws::SharedCredentials.new(profile_name: 'development')
  end

  def kinesis
    @kinesis ||= Aws::Kinesis::Client.new(region: 'eu-west-1', credentials: credentials)
  end

  def stream_exists?
    kinesis.describe_stream({ stream_name: stream_name })
    true
  rescue Aws::Kinesis::Errors::ResourceNotFoundException
    false
  end
end


