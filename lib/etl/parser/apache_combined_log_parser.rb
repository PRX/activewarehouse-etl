module ETL #:nodoc:
  module Parser #:nodoc:
    # Parser which can parser the Apache Combined Log Format as defined at
    # http://httpd.apache.org/docs/2.2/logs.html
    class ApacheCombinedLogParser < ETL::Parser::Parser
      include HttpTools

      def initialize(source, options={})
        super
      end

      def each
        Dir.glob(file).each do |file|
          File.open(file).each_line do |line|
            yield parse(line)
          end
        end
      end
      
      def parse(line)

        field_names = [:ip_address, :identd, :user, :timestamp, :request, :response_code, :bytes, :referrer, :user_agent]

        fields = field_names.inject({}){|h, n| h[n] = nil; h}

        line = line.gsub('\"', "'")

        # example line:  127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"
        if match_data = line.match(/^(\S+)\s(\S+)\s(\S+)\s\[([^\]]*)\]\s"([^"]*)"\s(\d*)\s(\S*)\s"([^"]*)"\s"([^"]*)"$/)

          field_names.each_with_index{|n, i| fields[n] = match_data[i+1]}

          fields[:bytes] = fields[:bytes].to_i

          #fields[:timestamp] =~ r%{(\d\d)/(\w\w\w)/(\d\d\d\d):(\d\d):(\d\d):(\d\d) -(\d\d\d\d)}
          d = Date._strptime(fields[:timestamp], '%d/%b/%Y:%H:%M:%S') unless fields[:timestamp].nil?
          fields[:timestamp] = Time.mktime(d[:year], d[:mon], d[:mday], d[:hour], d[:min], d[:sec], d[:sec_fraction]) unless d.nil?

          fields[:method], fields[:path] = fields[:request].split(/\s/) unless fields[:request].nil?

          # fields.merge!(parse_user_agent(fields[:user_agent])) unless fields[:user_agent].nil?
          # fields.merge!(parse_uri(fields[:referrer], :prefix => 'referrer_')) unless fields[:referrer].nil?
          
          fields.each{|k, v| fields[k] = nil if v == '-'}
        end
        fields
      end
      
    end
  end
end