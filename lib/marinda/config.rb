#############################################################################
## Classes for loading and representing configuration data for the local
## and global servers.
##
## --------------------------------------------------------------------------
## Copyright (C) 2007, 2008, 2009 The Regents of the University of California.
## 
## This file is part of Marinda.
## 
## Marinda is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## Marinda is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with Marinda.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

require 'yaml'

module Marinda

class ConfigBase

  class MalformedConfigException < RuntimeError; end

  def import_required(config, key, type, expand_path=false)
    import_scalar config, key, type, true, expand_path
  end

  def import_optional(config, key, type, expand_path=false)
    import_scalar config, key, type, false, expand_path
  end

  def import_scalar(config, key, type, required=true, expand_path=false)
    if type == TrueClass || type == FalseClass
      types = [ TrueClass, FalseClass ]
    else
      types = [ type ]
    end

    if config[key] != nil  # if key exists and the value isn't empty
      value = config[key]
      if types.any? { |t| value.kind_of? t }
        if expand_path && value !~ /^\s*\//
          value = File.dirname($options.config_path) + "/" + value 
        end
        instance_variable_set "@#{key}", value
      else
        raise MalformedConfigException, "'#{key}' has the wrong type: " +
          value.class.name + " instead of " + type.name
      end
    else
      if required
        raise MalformedConfigException, "missing required config item '#{key}'"
      end
    end
  end

end


#============================================================================

class GlobalConfig < ConfigBase

  attr_reader :server_port, :nodes, :use_ssl, :check_client_name, :cert, :key
  attr_reader :ca_file, :ca_path
  attr_reader :use_judy

  # Can raise MalformedConfigException and YAML exceptions.
  def initialize(path)
    config = YAML.load_file path
    $log.debug "%p", config if $options.verbose

    import_required config, "server_port", Integer
    import_nodes config

    import_optional config, "use_ssl", TrueClass
    if @use_ssl
      @check_client_name = true
      import_optional config, "check_client_name", TrueClass
      import_required config, "cert", String, true
      import_required config, "key", String, true
      import_optional config, "ca_file", String, true
      import_optional config, "ca_path", String, true

      unless @ca_file || @ca_path
        raise MalformedConfigException, "need either ca_file or ca_path"
      end
    end

    # Use Judy arrays instead of List.
    import_optional config, "use_judy", TrueClass

    # select processing in GlobalSpace
    import_optional config, "debug_io_select", TrueClass

    # read_data and write_data in GlobalSpace
    import_optional config, "debug_io_bytes", TrueClass

    # process_mux_message in GlobalSpace
    import_optional config, "debug_io_messages", TrueClass

    # command/request handling in GlobalSpace
    import_optional config, "debug_commands", TrueClass
  end


  def export_debugging_flags
    $debug_io_select = @debug_io_select
    $debug_io_bytes = @debug_io_bytes
    $debug_io_messages = @debug_io_messages
    $debug_commands = @debug_commands
  end


  private #----------------------------------------------------------------

  def import_nodes(config)
    key = "nodes"
    if config[key] != nil  # if key exists and the value isn't empty
      if config[key].instance_of? Hash
        if config[key].all? { |k,v| k.instance_of?(String) &&
                                        v.kind_of?(Integer) }
          validate_node_map config[key]
          @nodes = config[key]
        else
          raise MalformedConfigException, "invalid 'nodes' mapping: " +
            "wrong entry type: mappings must be from IP address to node ID"
        end
      else
        raise MalformedConfigException, "invalid 'nodes' mapping: config " +
          "item has the wrong YAML type: " + config[key].class.name +
          " instead of Hash"
      end
    else
      raise MalformedConfigException, "missing required config item 'nodes'"
    end

    $log.info "begin node map"
    @nodes.sort {|a, b| a[1] <=> b[1]}.each do |address, node_id|
      $log.info "%3d => %p", node_id, address
    end
    $log.info "end node map"
  end


  def validate_node_map(mappings)
    seen_node_id = {}    # node_id => true
    mappings.each do |address, node_id|
      unless address =~ /^\d+\.\d+\.\d+\.\d+$/
        raise MalformedConfigException, "invalid 'nodes' mapping: '" +
          address + "' is not an IP address"
      end

      if node_id <= 0 || node_id > 2**15
        raise MalformedConfigException, "invalid 'nodes' mapping: node_id " +
          "must be >= 1 and <= " + (2**15).to_s
      end

      if seen_node_id[node_id]
        raise MalformedConfigException, "invalid 'nodes' mapping: node_id " +
          node_id.to_s + " appears more than once"
      end
      seen_node_id[node_id] = true
    end
  end

end


#============================================================================

class LocalConfig < ConfigBase

  attr_reader :socket, :socket_access, :node_id, :localspace_only
  attr_reader :global_server_addr, :global_server_port
  attr_reader :use_ssl, :check_server_name, :cert, :key, :ca_file, :ca_path
  attr_reader :use_judy

  # Can raise MalformedConfigException and YAML exceptions.
  def initialize(path)
    config = YAML.load_file path
    $log.debug "%p", config if $options.verbose

    import_required config, "socket", String

    # Unix permissions for the server Unix socket.
    # Note: YAML accepts octal with 0 prefix.
    import_optional config, "socket_access", Integer
    if @socket_access && (@socket_access < 0 || @socket_access > 0777)
      raise MalformedConfigException, "invalid 'socket_access': " +
        "must be a valid Unix permission (e.g., 0600 for rw-------)"
    end

    import_required config, "node_id", Integer
    if @node_id <= 0 || @node_id > 2**15
      raise MalformedConfigException, "invalid 'node_id': " +
        "must be >= 1 and <= " + (2**15).to_s
    end

    import_optional config, "localspace_only", TrueClass
    unless @localspace_only
      import_required config, "global_server_addr", String
      import_required config, "global_server_port", Integer

      import_optional config, "use_ssl", TrueClass
      @use_ssl = false if @global_server_addr == "127.0.0.1"
      if @use_ssl
        @check_server_name = true
        import_optional config, "check_server_name", TrueClass
        import_required config, "cert", String, true
        import_required config, "key", String, true
        import_optional config, "ca_file", String, true
        import_optional config, "ca_path", String, true

        unless @ca_file || @ca_path
          raise MalformedConfigException, "need either ca_file or ca_path"
        end
      end
    end

    # Use Judy arrays instead of List.
    import_optional config, "use_judy", TrueClass

    # select processing in GlobalSpaceMux
    import_optional config, "debug_io_select", TrueClass

    # read_data and write_data in GlobalSpaceMux
    import_optional config, "debug_mux_io_bytes", TrueClass

    # process_mux_message in GlobalSpaceMux
    import_optional config, "debug_mux_io_messages", TrueClass

    # command/request handling in GlobalSpaceMux
    import_optional config, "debug_mux_commands", TrueClass

    # read_data and write_data in ClientIO
    import_optional config, "debug_client_io_bytes", TrueClass

    # command/request handling in Channel
    import_optional config, "debug_client_commands", TrueClass
  end


  def export_debugging_flags
    $debug_io_select = @debug_io_select
    $debug_mux_io_bytes = @debug_mux_io_bytes
    $debug_mux_io_messages = @debug_mux_io_messages
    $debug_mux_commands = @debug_mux_commands
    $debug_client_io_bytes = @debug_client_io_bytes
    $debug_client_commands = @debug_client_commands
  end

end

end  # module Marinda
