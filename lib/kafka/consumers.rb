
module Kafka
  # A client that consumes message from multiple consumers from a Kafka cluster
  # simultaneously
  #
  # ## Example
  #
  #     require "kafka"
  #
  #     # Create a new Consumer instance in the group `my-group`:
  #     consumer1 = kafka.consumer(group_id: "my-group")
  #     consumer1.subscribe("messages")
  #
  #     # Create a new Consumer instance in the group `my-group`:
  #     consumer2 = kafka.consumer(group_id: "my-group")
  #     consumer2.subscribe("messages")
  #
  #     # Loop forever, reading in messages from all consumers
  #     consumers = kafka.consumers
  #     consumers.add(consumer1)
  #     consumers.add(consumer2)
  #     consumers.each_message do |message, consumer|
  #       puts message.topic
  #       puts message.partition
  #       puts message.key
  #       puts message.headers
  #       puts message.value
  #       puts message.offset
  #     end
  #
  class Consumers
    include Enumerable

    attr_reader :consumers

    def initialize(logger:)
      @logger = logger
      @consumers = {}
    end

    # Add the consumer to the group to fetch messages for.
    #
    # @param consumer [Consumer] the consumer to fetch messages for.
    # @return [self]
    def add(consumer)
      group_id = consumer.group_id

      # Remove existing if any
      delete(group_id: group_id) if @consumers.key?(group_id)

      # Add consumer
      @consumers[group_id] = consumer

      self
    end

    # Removes the consumer from the group.
    #
    # @param group_id [String] the consumer group's group_id.
    # @return [Consumer]
    def delete(group_id:)
      consumer = @consumers.delete(group_id)
      consumer
    end

    # Returns true if a consumer with the specified group_id is part of this
    # group.
    #
    # @param group_id [String] the consumer group's group_id.
    # @return [boolean]
    def include?(group_id:)
      @consumers.include?(group_id)
    end

    # Retrieves the consumer for the specified group_id. Returns `nil` if the
    # consumer does not exist.
    #
    # @param group_id [String] the consumer group's group_id.
    # @return [boolean]
    def [](group_id)
      @consumers[group_id]
    end

    # Enumerates through the list of consumers.
    #
    # @return [nil]
    def each(&block)
      @consumers.each(&block)
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each message is yielded to the provided block. If the block returns
    # without raising an exception, the message will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from each broker; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_bytes [Integer] the maximum number of bytes to read before
    #   returning messages from each broker.
    # @param max_wait_time [Integer, Float] the maximum duration of time to wait before
    #   returning messages from each broker, in seconds.
    # @param automatically_mark_as_processed [Boolean] whether to automatically
    #   mark a message as successfully processed when the block returns
    #   without an exception. Once marked successful, the offsets of processed
    #   messages can be committed to Kafka.
    # @yieldparam message [Kafka::FetchedMessage] a message fetched from Kafka.
    # @yieldparam consumer [Kafka::Consumer] a the consumer that the message is for.
    # @raise [Kafka::ProcessingError] if there was an error processing a message.
    #   The original exception will be returned by calling `#cause` on the
    #   {Kafka::ProcessingError} instance.
    # @return [nil]
    def each_message(min_bytes: 1, max_bytes: 10485760, max_wait_time: 1, automatically_mark_as_processed: true)
      consumer_loop do |consumer, batches|
        consumer.process_message(
          batches,
          automatically_mark_as_processed: automatically_mark_as_processed,
        ) do |message|
          yield(message, consumer)
        end
      end
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each batch of messages is yielded to the provided block. If the block returns
    # without raising an exception, the batch will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message batch in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from each broker; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_bytes [Integer] the maximum number of bytes to read before
    #   returning messages from each broker.
    # @param max_wait_time [Integer, Float] the maximum duration of time to wait before
    #   returning messages from each broker, in seconds.
    # @param automatically_mark_as_processed [Boolean] whether to automatically
    #   mark a batch's messages as successfully processed when the block returns
    #   without an exception. Once marked successful, the offsets of processed
    #   messages can be committed to Kafka.
    # @yieldparam batch [Kafka::FetchedBatch] a message batch fetched from Kafka.
    # @yieldparam consumer [Kafka::Consumer] a the consumer that the message is for.
    # @return [nil]
    def each_batch(min_bytes: 1, max_bytes: 10485760, max_wait_time: 1, automatically_mark_as_processed: true)
      @fetcher.configure(
        min_bytes: min_bytes,
        max_bytes: max_bytes,
        max_wait_time: max_wait_time,
      )

      consumer_loop do |consumer, batches|
        consumer.process_batch(
          batches,
          automatically_mark_as_processed: automatically_mark_as_processed,
        ) do |batch|
          yield(batch, consumer)
        end
      end
    end

    # Stop the consumers.
    #
    # The consumers will finish any in-progress work and shut down.
    #
    # @return [nil]
    def stop
      self.running = false
    end

  protected
    def consumer_loop
      self.running = true

      while @running
        begin
          @consumers.each do |_, consumer|
            consumer.step do |batches|
              yield consumer, batches
            end
          end
        rescue SignalException => e
          @logger.warn "Received signal #{e.message}, shutting down"
          self.running = false
        end
      end
    ensure
      @consumers.each do |_, consumer|
        consumer.finalize
      end
      self.running = false
    end

    def running=(running)
      @running = running
      @consumers.each do |_, c|
        c.running = running
      end
    end


    def consumer_assigned_to(topic, partition)
      @consumers.each do |_, consumer|
        if consumer.assigned_to?(topic, partition)
          yield consumer
          return
        end
      end
    end
  end
end
