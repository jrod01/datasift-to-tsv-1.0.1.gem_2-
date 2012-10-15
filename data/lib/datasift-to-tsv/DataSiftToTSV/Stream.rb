
class DataSiftToTSV::Stream

  attr_reader :cdsl

  include DataSiftToTSV::Configuration

  ################
  #  initialize  #
  ################

  def initialize( search_descriptor )

    @cdsl = search_descriptor

    load_configuration
    authenticate

    init_signals

    create_datasift_stream

  end

  ##################
  #  init_signals  #
  ##################

  def init_signals

    Signal.trap( 'USR1' ) do
      if ! @running
        enable
      elsif @running
        disable
      end
    end

		Signal.trap( 'TERM' ) do
			@stream.stop()
			exit
		end

  end

  ##################################################################################################
  #######################################  Stream Setup  ###########################################
  ##################################################################################################

  ############
  #  enable  #
  ############

  def enable

    create_datasift_stream
		consume_datasift_stream

  end

  #############
  #  disable  #
  #############

  def disable

    @running = false

  end

  #############
  #  enabled  #
  #############

  def enabled?

    return @running

  end

  ##############
  #  disabled  #
  ##############

  def disabled?

    return ! @running

  end

  ##################
  #  authenticate  #
  ##################

  def authenticate

    @user = DataSift::User.new( @configuration['username'], @configuration['api-key'] )

  end

  ############################
  #  create_datasift_stream  #
  ############################

  def create_datasift_stream

    ensure_stream_directory_exists

    @definition = @user.createDefinition( @cdsl )

    unless @definition.hash
      raise ArgumentError, 'Malformed CDSL.'
    end

    @stream = @definition.getConsumer( ::DataSift::StreamConsumer::TYPE_HTTP )

  end

  ####################################
  #  ensure_stream_directory_exists  #
  ####################################

  def ensure_stream_directory_exists

    # make sure general storage directory exists (where all stream outputs are stored)
    unless File.exist?( directory__storage )
      Dir.mkdir( directory__storage )
    end

    # create stream output directory if necessary
    unless File.exist?( directory__stream_outputs( @cdsl ) )
      Dir.mkdir( directory__stream_outputs( @cdsl ) )
    end

  end

  ##################################################################################################
  #####################################  Stream Processing  ########################################
  ##################################################################################################

  #############################
  #  consume_datasift_stream  #
  #############################

  def consume_datasift_stream

    @running = true

    @stream.consume( true ) do |interaction|
      break unless @running
    	if interaction

        if ! @day_streamer
          @day_streamer = ::DataSiftToTSV::Stream::Day.new( self )
        elsif @day_streamer.date.to_s != Date.today.to_s
          invoke_post_processing
          @day_streamer = ::DataSiftToTSV::Stream::Day.new( self )
        end

        @day_streamer.record_stream_element( interaction )

    	end
    end

  end
  alias_method :consume, :consume_datasift_stream

  ############################
  #  invoke_post_processing  #
  ############################

  def invoke_post_processing

    # fork a process so we don't have to wait for it to finish
    pid = fork do
      ::DataSiftToTSV::Stream::PostProcessor.new( @cdsl, @day_streamer.date ).process_output_files
    end

    # we don't care about process exit status - don't block
    Process.detach( pid )

  end

end
