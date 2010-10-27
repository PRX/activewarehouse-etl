require 'fileutils'

module ETL #:nodoc:
  module Processor #:nodoc:
    class EscapeCsvProcessor < ETL::Processor::Processor
      
      # The file to load from
      attr_reader :source_file
      # The file to write to
      attr_reader :target_file
      # whether to use a temporary file or not
      attr_reader :use_temp_file

      attr_reader :filters

      attr_reader :buffer
      
      # Initialize the processor.
      #
      # Configuration options:
      # * <tt>:source_file</tt>: The file to load data from
      # * <tt>:target_file</tt>: The file to write data to
      # * <tt>:file</tt>: short-cut which will set the same value to both source_file and target_file
      def initialize(control, configuration)
        super
        if configuration[:file]
          @use_temp_file = true
          configuration[:source_file] = configuration[:file]
          configuration[:target_file] = configuration[:file] + '.tmp'
        end
        path = Pathname.new(configuration[:source_file])
        @source_file = path.absolute? ? path : Pathname.new(File.dirname(File.expand_path(configuration[:source_file]))) + path
        path = Pathname.new(configuration[:target_file])
        @target_file = path.absolute? ? path : Pathname.new(File.dirname(File.expand_path(configuration[:target_file]))) + path
        @filters = configuration[:filters] || [{:replace => '\"', :result => '""'}]
        @buffer = configuration[:buffer] || 8192
        raise ControlError, "Source file must be specified" if @source_file.nil?
        raise ControlError, "Target file must be specified" if @target_file.nil?
        raise ControlError, "Source and target file cannot currently point to the same file" if @source_file == @target_file
      end
      
      # Execute the processor
      def process
        reader = File.open(@source_file, 'r')
        writer = File.open(@target_file, 'w')

        while !reader.eof?
          reading = reader.readpartial(@buffer)
          @filters.each do |filter|
            result = reading.gsub(Regexp.new(filter[:replace]), filter[:result])
            reading = result
          end
          writer.write(reading)
        end

        reader.close
        writer.close

        if use_temp_file
          FileUtils.rm(source_file)
          FileUtils.mv(target_file,source_file)
        end
      end
    end
  end
end
