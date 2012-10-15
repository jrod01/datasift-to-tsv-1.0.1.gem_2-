
class DataSiftToTSV

  include DataSiftToTSV::Configuration

  StreamStruct = Struct.new( :cdsl, :enabled, :pid )

  ################
  #  initialize  #
  ################

  def initialize

    @enabled_streams_array = [ ]
    @disabled_streams_array = [ ]

    load_configuration

    # make sure storage directory exists
    ensure_stream_storage_directory_exists

    # check for existing streams and load them
    load_existing_streams

    # run loop until quit
    event_loop

  end

  ############################################
  #  ensure_stream_storage_directory_exists  #
  ############################################

  def ensure_stream_storage_directory_exists

    unless File.exists?( directory__configuration )
      Dir.mkdir( directory__configuration )
    end

    unless File.exists?( directory__stream_storage )
      Dir.mkdir( directory__stream_storage )
    end

  end

  ###########################
  #  load_existing_streams  #
  ###########################

  def load_existing_streams

    # load stream descriptors from file
    Dir[ directory__stream_storage + '/*' ].each do |this_file_name|
      File.open( this_file_name ) do |this_file|
        this_stream_struct = YAML.load( this_file.readlines.join )

        if this_stream_struct.enabled
          begin
            Process.kill( 0, this_stream_struct.pid ).to_s
            # only push if no exception
            @enabled_streams_array.push( this_stream_struct )
          rescue Errno::ESRCH
            # if we don't have an existing pid re-create the stream instance
            create_stream( this_stream_struct.cdsl )
          end
        else
          @disabled_streams_array.push( this_stream_struct )
        end

      end
    end

  end

  ###################
  #  create_stream  #
  ###################

  def create_stream( stream_cdsl_phrase )

    ( @enabled_streams_array + @disabled_streams_array ).each do |this_stream_struct|
      if this_stream_struct.cdsl == stream_cdsl_phrase
        say( 'Stream already exists for CDSL.' )
        return false
      end
    end

    begin
      stream = ::DataSiftToTSV::Stream.new( stream_cdsl_phrase )
    rescue ArgumentError => error
      say( error.message )
      return false
    end

    pid = fork { stream.consume }

    Process.detach( pid )

    stream_struct = StreamStruct.new( stream.cdsl, true, pid )

    @enabled_streams_array.push( stream_struct )

    write_stream_struct( stream_struct )

    return true

  end

  #########################
  #  write_stream_struct  #
  #########################

  def write_stream_struct( stream_struct )

    File.open( file__stream_struct( stream_struct ), 'w' ) do |file|
      file.write( stream_struct.to_yaml )
    end

  end

  ##########################
  #  delete_stream_struct  #
  ##########################

  def delete_stream_struct( stream_struct )

    if File.exists?( file__stream_struct( stream_struct ) )
      File.delete( file__stream_struct( stream_struct ) )
    end

  end

  ##################
  #  list_streams  #
  ##################

  def list_streams

    say( 'Enabled streams:' )
    list_enabled_streams

    say ( '' )

    say( 'Disabled streams:' )
    list_disabled_streams

  end

  ##########################
  #  list_enabled_streams  #
  ##########################

  def list_enabled_streams

    @enabled_streams_array.each_with_index do |this_stream, index|

      say( ( index + 1 ).to_s + ': ' + this_stream.cdsl.to_s + " (#{interaction_count( this_stream ).to_i} interactions)" )

    end.any? || say( "  -- none --" )

  end

  ###########################
  #  list_disabled_streams  #
  ###########################

  def list_disabled_streams

    @disabled_streams_array.each_with_index do |this_stream, index|

      say( ( index + 1 ).to_s + ': ' + this_stream.cdsl.to_s + " (#{interaction_count( this_stream ).to_i} interactions)" )

    end.any? || say( "  -- none --" )

  end

  ###################
  #  enable_stream  #
  ###################

  def enable_stream( stream_number )

    if ! @disabled_streams_array[ stream_number ]

      say( 'No disabled stream numbered ' + stream_number.to_s )

    else

      newly_enabled_stream = @disabled_streams_array.delete_at( stream_number )
      newly_enabled_stream.enabled = true
      begin
        Process.kill( 'USR1', newly_enabled_stream.pid )
      rescue Errno::ESRCH
      end
      @enabled_streams_array.push( newly_enabled_stream )
      write_stream_struct( newly_enabled_stream )

    end

  end

  ####################
  #  disable_stream  #
  ####################

  def disable_stream( stream_number )

    if ! @enabled_streams_array[ stream_number ]

      say( 'No enabled stream numbered ' + stream_number.to_s )

    else

      newly_disabled_stream = @enabled_streams_array.delete_at( stream_number )
      newly_disabled_stream.enabled = false
      begin
        Process.kill( 'USR1', newly_disabled_stream.pid )
      rescue Errno::ESRCH
      end
      @disabled_streams_array.push( newly_disabled_stream )
      write_stream_struct( newly_disabled_stream )

    end

  end

  ###################
  #  export_stream  #
  ###################

  def export_stream( stream_number )

    stream_to_export = nil

    # if we have an enabled stream and no disabled or vice-versa, delete it
    if ( @enabled_streams_array[ stream_number ] and ! @disabled_streams_array[ stream_number ] )

      stream_to_export = @enabled_streams_array.delete_at( stream_number )

    elsif ( @disabled_streams_array[ stream_number ] and ! @enabled_streams_array[ stream_number ] )

      stream_to_export = @disabled_streams_array.delete_at( stream_number )

    # otherwise ask which one
    else

      choose do |menu|

        menu.layout = :menu_only
        menu.shell = true

        menu.choice( :enabled ) do |command, details|

          stream_to_export = @enabled_streams_array.delete_at( stream_number )

        end

        menu.choice( :disabled ) do |command, details|

          stream_to_export = @disabled_streams_array.delete_at( stream_number )

        end

      end

    end

    if ! stream_to_export

      say( 'No stream numbered ' + stream_number.to_s )

    else

      post = ::DataSiftToTSV::Stream::PostProcessor.new( stream_to_export.cdsl, Date.today )

      pid = fork do
        post.process_output_files
      end

      Process.detach( pid )
      say( "  Export file: #{post.file__tsv_output_file}")
      exit

    end

  end
  ###################
  #  remove_stream  #
  ###################

  def remove_stream( stream_number )

    stream_to_remove = nil

    # if we have an enabled stream and no disabled or vice-versa, delete it
    if ( @enabled_streams_array[ stream_number ] and ! @disabled_streams_array[ stream_number ] )

      stream_to_remove = @enabled_streams_array.delete_at( stream_number )

    elsif ( @disabled_streams_array[ stream_number ] and ! @enabled_streams_array[ stream_number ] )

      stream_to_remove = @disabled_streams_array.delete_at( stream_number )

    # otherwise ask which one
    else

      choose do |menu|

        menu.layout = :menu_only
        menu.shell = true

        menu.choice( :enabled ) do |command, details|

          stream_to_remove = @enabled_streams_array.delete_at( stream_number )

        end

        menu.choice( :disabled ) do |command, details|

          stream_to_remove = @disabled_streams_array.delete_at( stream_number )

        end

      end

    end

    if ! stream_to_remove

      say( 'No stream numbered ' + stream_number.to_s )

    else

      begin
        Process.kill( 'TERM', stream_to_remove.pid )
      rescue Errno::ESRCH
      end

      delete_stream_struct( stream_to_remove )

    end

  end

  ################
  #  event_loop  #
  ################

  def event_loop

    loop do

      choose do |menu|

        #==============#
        #  Event Menu  #
        #==============#

        menu.layout = :menu_only

        menu.shell  = true

        #-----------#
        #  streams  #
        #-----------#

        menu.choice( :list, "List streams." ) do |command, details|
          if ( @enabled_streams_array + @disabled_streams_array ).empty?
            say( 'No existing streams.' )
          else
            say( 'Streams:' )
            list_streams
          end
        end

        #----------#
        #  create  #
        #----------#

        menu.choice( :create, 'Create [CDSL search phrase].' ) do |command, details|

          if details.empty?

            say( 'CDSL search phrase required.' )
            say( 'Usage: "Create [CDSL search phrase]"' )

          else

            say( 'Creating new search stream for CSDL search phrase: "' + details.to_s + '"' )
            create_stream( details )

          end

        end

        #----------#
        #  enable  #
        #----------#

        menu.choice( :enable, 'List enabled streams.' ) do |command, details|

          if details.empty?

            say( 'Enabled streams:' )
            list_enabled_streams

          else

            # make sure details specify a number
            if details =~ /\d+/

              say( 'Enabling stream #' + details.to_s )
              enable_stream( details.to_i - 1 )

            else

              say( 'Expected number to specify stream to enable.' )

            end

          end

        end

        #-----------#
        #  disable  #
        #-----------#

        menu.choice( :disable, 'List disabled streams.' ) do |command, details|

          if details.empty?

            say( 'Disabled streams:' )
            list_disabled_streams

          else

            # make sure details specify a number
            if details =~ /\d+/

              say( 'Disabling stream #' + details.to_s )
              disable_stream( details.to_i - 1 )

            else

              say( 'Expected number to specify stream to disable.' )

            end

          end

        end

        #----------#
        #  tsvify  #
        #----------#

        menu.choice( :tsvify, 'Export a stream\'s data from today to TSV.' ) do |command, details|

          if details.empty?

            say( 'Must specify stream # to export.' )
            list_streams

          else

            # make sure details specify a number
            if details =~ /\d+/

              say( 'Exporting stream #' + details.to_s )
              export_stream( details.to_i - 1 )

            else

              say( 'Expected number to specify stream to export.' )

            end

          end

        end

        #----------#
        #  remove  #
        #----------#

        menu.choice( :remove, 'Remove stream.' ) do |command, details|

          if details.empty?

            say( 'Must specify stream # to remove.' )
            list_streams

          else

            # make sure details specify a number
            if details =~ /\d+/

              say( 'Removing stream #' + details.to_s )
              remove_stream( details.to_i - 1 )

            else

              say( 'Expected number to specify stream to remove.' )

            end

          end

        end

        #--------#
        #  quit  #
        #--------#

        menu.choice( :quit, 'Exit program.' ) do

          exit

        end

      end

    end

  end

  ##################################################################################################
  ##################################  Directories and Files  #######################################
  ##################################################################################################

  #########################
  #  file__stream_struct  #
  #########################

  def file__stream_struct( stream_struct )

    return File.join( directory__stream_storage, name__cdsl_output( stream_struct.cdsl ) + '.yml' )

  end

  #######################
  #  interaction_count  #
  #######################

  def interaction_count( stream_struct )

    today_current_stream_item = nil

    begin
      File.open( file__next_stream_item_for_cdsl_and_date( stream_struct.cdsl, Date.today ), 'r' ) do |file|
        today_current_stream_item = YAML.load( file.readlines.join )
      end
    rescue
      nil
    end

    return today_current_stream_item
  end



end
