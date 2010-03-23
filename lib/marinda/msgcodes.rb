#############################################################################
## Constants used in the protocol between the client and the local server
## and between the local and global servers.
##
## --------------------------------------------------------------------------
## Author: Young Hyun
## Copyright (C) 2007,2008,2009,2010 The Regents of the University of
## California.
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

module Marinda

# NOTE: Never remove a message code from an enum once deployed, and always
#       add new codes to the end of each set.  Otherwise, the numeric value
#       of message codes will change and break protocol interoperability.
module ChannelMessageCodes

  # Protocol 3:
  #  * switched to using MIO (instead of YAML) for encoding tuples/templates
  #  * HELLO_RESP returns more info: client_id, run_id, node_id, node_name
  #
  # Protocol 2b:
  #  * added TAKE_ALL_CMD, CONSUME_CMD, MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
  #
  # Protocol 2:
  #  * removed DUMP_CMD
  PROTOCOL_VERSION = 3

  MAX_SERVICE_HANDLE = 1024
  FIRST_USER_HANDLE = 1025

  # client to tuple space commands ----------------------------------------

  CLIENT_COMMANDS = {
    0 => :INVALID_CMD,
    1 => :HELLO_CMD,
    2 => :WRITE_CMD,
    3 => :REPLY_CMD,
    4 => :REMEMBER_CMD,
    5 => :FORGET_CMD,
    6 => :WRITE_TO_CMD,
    7 => :FORWARD_TO_CMD,
    8 => :PASS_ACCESS_TO_CMD,
    9 => :READ_CMD,
    10 => :READP_CMD,
    11 => :TAKE_CMD,
    12 => :TAKEP_CMD,
    13 => :TAKE_PRIV_CMD,
    14 => :TAKEP_PRIV_CMD,
    15 => :MONITOR_CMD,
    16 => :READ_ALL_CMD,
    17 => :NEXT_CMD,
    18 => :CANCEL_CMD,
    19 => :CREATE_NEW_BINDING_CMD,
    20 => :DUPLICATE_CHANNEL_CMD,
    21 => :CREATE_GLOBAL_COMMONS_CHANNEL_CMD,
    22 => :OPEN_PORT_CMD,
    23 => :TAKE_ALL_CMD,
    24 => :CONSUME_CMD,
    25 => :MONITOR_STREAM_CMD,
    26 => :CONSUME_STREAM_CMD
  }

  CLIENT_COMMANDS.each do |value, key|
    const_set key, value
  end

  # tuple space to client responses ---------------------------------------

  CHANNEL_RESPONSES = {
    0 => :ERROR_RESP,
    1 => :HELLO_RESP,
    2 => :ACK_RESP,
    3 => :TUPLE_RESP,
    4 => :TUPLE_NIL_RESP,
    5 => :TUPLE_WITH_RIGHTS_RESP,
    6 => :ACCESS_RIGHT_RESP,
    7 => :HANDLE_RESP
  }

  CHANNEL_RESPONSES.each do |value, key|
    const_set key, value
  end

  # error subcodes in server responses ------------------------------------

  ERROR_SUBCODES = {
    0 => :ERRORSUB_UNKNOWN_RESPONSE,
    1 => :ERRORSUB_MALFORMED_MESSAGE,
    2 => :ERRORSUB_PROTOCOL_NOT_SUPPORTED,
    3 => :ERRORSUB_NO_SPACE,
    4 => :ERRORSUB_NO_PRIVILEGE,
    5 => :ERRORSUB_NOT_SUPPORTED
  }

  ERROR_SUBCODES.each do |value, key|
    const_set key, value
  end

end

#===========================================================================

module MuxMessageCodes

  # Protocol 3:
  #  * switched to using MIO (instead of YAML) for encoding tuples/templates
  #  * added HEARTBEAT_RESP
  #
  # Protocol 2b:
  #  * added TAKE_ALL_CMD, CONSUME_CMD, MONITOR_STREAM_CMD, CONSUME_STREAM_CMD
  #
  # Protocol 2:
  #  * added session ID and node ID to HELLO_CMD & HELLO_RESP
  #  * removed DUMP_CMD
  PROTOCOL_VERSION = 3

  MAX_SERVICE_HANDLE = 1024
  FIRST_USER_HANDLE = 1025

  # mux to demux commands -------------------------------------------------

  MUX_COMMANDS = {
    0 => :INVALID_CMD,
    1 => :HELLO_CMD,
    2 => :HEARTBEAT_CMD,
    3 => :WRITE_CMD,
    4 => :READ_CMD,
    5 => :READP_CMD,
    6 => :TAKE_CMD,
    7 => :TAKEP_CMD,
    8 => :MONITOR_CMD,
    9 => :READ_ALL_CMD,
    10 => :CANCEL_CMD,
    11 => :ACK_CMD,
    12 => :FIN_CMD,
    13 => :CREATE_PRIVATE_REGION_CMD,
    14 => :DELETE_PRIVATE_REGION_CMD,
    15 => :CREATE_REGION_PAIR_CMD,
    16 => :TAKE_ALL_CMD,
    17 => :CONSUME_CMD,
    18 => :MONITOR_STREAM_CMD,
    19 => :CONSUME_STREAM_CMD
  }

  MUX_COMMANDS.each do |value, key|
    const_set key, value
  end

  # demux to mux responses ------------------------------------------------

  DEMUX_RESPONSES = {
    0 => :ERROR_RESP,
    1 => :HELLO_RESP,
    2 => :ACK_RESP,
    3 => :PORT_RESP,
    4 => :TUPLE_RESP,
    5 => :TUPLE_NIL_RESP,
    6 => :HEARTBEAT_RESP
  }

  DEMUX_RESPONSES.each do |value, key|
    const_set key, value
  end

  # error subcodes in server responses ------------------------------------

  ERROR_SUBCODES = {
    0 => :ERRORSUB_UNKNOWN_RESPONSE,
    1 => :ERRORSUB_MALFORMED_MESSAGE,
    2 => :ERRORSUB_PROTOCOL_NOT_SUPPORTED,
    3 => :ERRORSUB_NO_SPACE,
    4 => :ERRORSUB_NO_PRIVILEGE,
    5 => :ERRORSUB_NOT_SUPPORTED
  }

  ERROR_SUBCODES.each do |value, key|
    const_set key, value
  end

end

end  # module Marinda
