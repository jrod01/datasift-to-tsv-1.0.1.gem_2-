
require 'rubygems'

require 'datasift'
require 'yaml'
require 'date'

require 'highline/import'

$terminal.wrap_at = 120
$terminal.page_at = 22

class DataSiftToTSV
  module Configuration
  end
  class Stream
    class Day
    end
    class PostProcessor
    end
  end
end

base_path = File.expand_path( File.dirname( __FILE__ ) )

require File.join( base_path, 'datasift-to-tsv/DataSiftToTSV/Configuration.rb' )
require File.join( base_path, 'datasift-to-tsv/DataSiftToTSV/Stream/Day.rb' )
require File.join( base_path, 'datasift-to-tsv/DataSiftToTSV/Stream/PostProcessor.rb' )
require File.join( base_path, 'datasift-to-tsv/DataSiftToTSV/Stream.rb' )
require File.join( base_path, 'datasift-to-tsv/DataSiftToTSV.rb' )

# Core extension. Wasn't loading from a separate file.
class String
  def parameterize(sep = '-')
    # Turn unwanted chars into the separator
    parameterized_string = self.gsub(%r{[^a-z0-9\-_]+}, sep)
    parameterized_string.downcase
  end
end
