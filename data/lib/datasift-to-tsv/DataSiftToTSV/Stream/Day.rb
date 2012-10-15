
class DataSiftToTSV::Stream::Day

  include DataSiftToTSV::Configuration

  attr_reader :date

  ################
  #  initialize  #
  ################

  def initialize( parent_stream )

    @parent_stream = parent_stream
    @date = Date.today

    load_configuration

    ensure_today_directory_exists
    init_or_load_today_current_stream_item
    init_or_load_accumulated_indexes

  end

  ##################################################################################################
  #########################################  Day Setup  ############################################
  ##################################################################################################

  ###################################
  #  ensure_today_directory_exists  #
  ###################################

  def ensure_today_directory_exists

    unless File.exist?( directory__dated_stream_output( @parent_stream.cdsl, @date ) )
      Dir.mkdir( directory__dated_stream_output( @parent_stream.cdsl, @date ) )
    end

  end

  ######################################
  #  init_or_load_accumulated_indexes  #
  ######################################

  def init_or_load_accumulated_indexes

    # we only check for one because we always write both together
    if File.exist?( file__column_indexes_array( @parent_stream.cdsl, @date ) )

      File.open( file__column_indexes_hash( @parent_stream.cdsl, @date ), 'r' ) do |file|
        @today_index_descriptions_hash = YAML.load( file.readlines.join )
      end
      File.open( file__column_indexes_array( @parent_stream.cdsl, @date ), 'r' ) do |file|
        @today_index_descriptions_array = YAML.load( file.readlines.join )
      end

    else

      # make sure the base directory for today exists

      @today_index_descriptions_array = [ ]
      @today_index_descriptions_hash = { }

      write_accumulated_indexes_to_file

    end

  end

  ############################################
  #  init_or_load_today_current_stream_item  #
  ############################################

  def init_or_load_today_current_stream_item

    if File.exist?( file__next_stream_item_for_cdsl_and_date( @parent_stream.cdsl, @date ) )

      File.open( file__next_stream_item_for_cdsl_and_date( @parent_stream.cdsl, @date ), 'r' ) do |file|
        @today_current_stream_item = YAML.load( file.readlines.join )
      end

    else

      init_today_current_stream_item

    end

  end

  def init_today_current_stream_item

    @today_current_stream_item = 0

    write_current_stream_item_to_file

  end

  ##################################################################################################
  #####################################  Stream Recording  #########################################
  ##################################################################################################

  ###########################
  #  record_stream_element  #
  ###########################

  def record_stream_element( interaction )

    flattened_key_elements_hash = collect_flattened_element_data( interaction )

    write_stream_element_to_file( flattened_key_elements_hash )

  end

  ####################################
  #  collect_flattened_element_data  #
  ####################################

  def collect_flattened_element_data( interaction_hash, cumulative_name = [ ], flattened_key_elements_hash = { } )

    interaction_hash.each do |this_key, this_data|

      this_cumulative_name = cumulative_name.dup
      this_cumulative_name.push( this_key )

      # look and see if we have already indexed this category
      unless index = @today_index_descriptions_hash[ this_cumulative_name ]
        index = @today_index_descriptions_array.count
        @today_index_descriptions_hash[ this_cumulative_name ] = index
        @today_index_descriptions_array.push( this_cumulative_name )
        write_accumulated_indexes_to_file
      end

      if this_data.is_a?( Hash )
        flattened_sub_data = collect_flattened_element_data( this_data,
                                                             this_cumulative_name,
                                                             flattened_key_elements_hash )
        flattened_key_elements_hash.merge( flattened_sub_data )
      else
        flattened_key_elements_hash[ this_cumulative_name ] = this_data
      end
    end

    return flattened_key_elements_hash

  end

  ##################################
  #  write_stream_element_to_file  #
  ##################################

  def write_stream_element_to_file( flattened_key_elements_hash )

    # write this interaction to file
    File.open( file__stream_item( @parent_stream.cdsl, @date, @today_current_stream_item ), 'w' ) do |file|
      file.write( flattened_key_elements_hash.to_yaml )
    end

    # iterate the counter for next file we write to
    @today_current_stream_item += 1
    write_current_stream_item_to_file

  end

  #######################################
  #  write_current_stream_item_to_file  #
  #######################################

  def write_current_stream_item_to_file

    File.open( file__next_stream_item_for_cdsl_and_date( @parent_stream.cdsl, @date ), 'w' ) do |file|
      file.write( @today_current_stream_item.to_yaml )
    end

  end

  #######################################
  #  write_accumulated_indexes_to_file  #
  #######################################

  def write_accumulated_indexes_to_file

    File.open( file__column_indexes_array( @parent_stream.cdsl, @date ), 'w' ) do |file|
      file.write( @today_index_descriptions_array.to_yaml )
    end
    File.open( file__column_indexes_hash( @parent_stream.cdsl, @date  ), 'w' ) do |file|
      file.write( @today_index_descriptions_hash.to_yaml )
    end

  end

end
