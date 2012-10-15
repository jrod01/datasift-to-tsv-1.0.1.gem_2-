
module DataSiftToTSV::Configuration

  ########################
  #  load_configuration  #
  ########################

  def load_configuration

    yaml = nil
    File.open( file__configuration ) do |file|
      yaml = file.readlines.join
    end

    @configuration = YAML::load( yaml )

    @configuration[ 'storage-directory' ] = File.expand_path( @configuration[ 'storage-directory' ] )

  end

  ##################################################################################################
  ##################################  Directories and Files  #######################################
  ##################################################################################################

  ##############################
  #  directory__configuration  #
  ##############################

  def directory__configuration

    return File.join( File.expand_path( '~' ), file__directory_name )

  end

  ###############################
  #  directory__stream_outputs  #
  ###############################

  def directory__stream_outputs( cdsl )

    output_path = File.join( directory__storage, name__cdsl_output( cdsl ) )

    return output_path

  end

  ###############################
  #  directory__stream_storage  #
  ###############################

  def directory__stream_storage

    return File.join( directory__configuration, 'streams' )

  end


  ####################################
  #  directory__dated_stream_output  #
  ####################################

  def directory__dated_stream_output( cdsl, date )

    return File.join( directory__stream_outputs( cdsl ), date.to_s )

  end

  ###############################
  #  file__column_indexes_hash  #
  ###############################

  def file__column_indexes_hash( cdsl, date )

    return File.join( directory__dated_stream_output( cdsl, date ), name__index_hash_file )

  end

  ################################
  #  file__column_indexes_array  #
  ################################

  def file__column_indexes_array( cdsl, date )

    return File.join( directory__dated_stream_output( cdsl, date ), name__index_array_file )

  end

  #########################
  #  file__configuration  #
  #########################

  def file__configuration

    return File.join( File.expand_path( '~' ), file__config_file_name )

  end

  ############################
  #  file__config_file_name  #
  ############################

  def file__config_file_name

    return file__directory_name + '.yml'

  end

  ##########################
  #  file__directory_name  #
  ##########################

  def file__directory_name

    return '.datasift.config'

  end

  ##############################################
  #  file__next_stream_item_for_cdsl_and_date  #
  ##############################################

  def file__next_stream_item_for_cdsl_and_date( cdsl, date )

    return File.join( directory__dated_stream_output( cdsl, date ), name__file_with_next_stream_item )

  end

  #######################
  #  file__stream_item  #
  #######################

  def file__stream_item( cdsl, date, number )

    return File.join( directory__dated_stream_output( cdsl, date ), name__current_stream_item( number ) )

  end

  ########################
  #  directory__storage  #
  ########################

  def directory__storage

    return @configuration[ 'storage-directory' ]

  end

  #######################
  #  name__cdsl_output  #
  #######################

  def name__cdsl_output( cdsl )

    return cdsl.parameterize

  end

  ############################
  #  name__index_array_file  #
  ############################

  def name__index_array_file

    return 'name__index_array.yml'

  end

  ###########################
  #  name__index_hash_file  #
  ###########################

  def name__index_hash_file

    return 'name__index_hash.yml'

  end

  ######################################
  #  name__file_with_next_stream_item  #
  ######################################

  def name__file_with_next_stream_item

    return 'current_stream_item.yml'

  end

  ###############################
  #  name__current_stream_item  #
  ###############################

  def name__current_stream_item( number )

    return number.to_s + '.yml'

  end

end
