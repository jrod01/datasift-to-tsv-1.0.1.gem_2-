
class DataSiftToTSV::Stream::PostProcessor

  include DataSiftToTSV::Configuration

  ################
  #  initialize  #
  ################

  def initialize( cdsl, date )

    @cdsl = cdsl
    @date = date

    load_configuration

    unless File.exists?( directory__dated_stream_output( cdsl, date ) ) and
           File.exists?( file__column_indexes_hash( cdsl, date ) ) and
           File.exists?( file__column_indexes_array( cdsl, date ) ) and
           File.exists?( file__next_stream_item_for_cdsl_and_date( @cdsl, @date ) )
      raise 'Dated stream output and associated information not found for ' + date.to_s
    end

    File.open( file__column_indexes_hash( cdsl, date ) ) do |file|
      @indexes_hash = YAML.load( file.readlines.join )
    end

    File.open( file__column_indexes_array( cdsl, date ) ) do |file|
      @indexes_array = YAML.load( file.readlines.join )
    end

    @titles_array = [ ]

    initialize_category_titles_array

  end

  ######################################
  #  initialize_category_titles_array  #
  ######################################

  def initialize_category_titles_array

    @indexes_array.each_with_index do |this_element, index|
      @titles_array[ index ] = this_element.join( '-' )
    end

  end

  ##########################
  #  process_output_files  #
  ##########################

  def process_output_files

    # get output count for current stream item
    current_stream_item = nil
    File.open( file__next_stream_item_for_cdsl_and_date( @cdsl, @date ) ) do |file|
      current_stream_item = YAML.load( file.readlines.join )
    end

    File.open( file__tsv_output_file, 'w' ) do |output_tsv_file|

      column_headers = @titles_array.join( "\t" )
      output_tsv_file.puts( column_headers )

      current_stream_item.times do |this_time|
        File.open( file__stream_item( @cdsl, @date, this_time ) ) do |this_file|

          this_interaction = YAML.load( this_file.readlines.join )

          output_line_array = process_interaction( this_interaction )

          output_tsv_line = output_line_array.join( "\t" )
          output_tsv_file.puts( output_tsv_line )

        end
      end

    end

  end

  ##########################
  #  process_output_files  #
  ##########################

  def process_interaction( interaction )

    output_line_array = [ ]

    interaction.each do |this_interaction_cumulative_name, this_interaction_element_data|
      column_index = @indexes_hash[ this_interaction_cumulative_name ]
      output_line_array[ column_index ] = this_interaction_element_data
    end

    return output_line_array

  end

  ##################################################################################################
  ##################################  Directories and Files  #######################################
  ##################################################################################################

  ###########################
  #  file__tsv_output_file  #
  ###########################

  def file__tsv_output_file

    return File.join( directory__stream_outputs( @cdsl ), name__tsv_output_file )

  end

  ###########################
  #  name__tsv_output_file  #
  ###########################

  def name__tsv_output_file

    return @date.to_s + '.' + name__cdsl_output( @cdsl ).to_s + '.tsv'

  end

end
