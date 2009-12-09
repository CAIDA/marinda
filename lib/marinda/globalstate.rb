#############################################################################
## A class for managing the persistent state of the global server.
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

require 'amalgalite'

require 'marinda/port'

module Marinda

class GlobalState

  attr_reader :db, :checkpoint_txn, :checkpoint_timestamp, :checkpoint_id

  def initialize(state_db_path)
    @db = open_state_database state_db_path
  end


  def get_last_public_portnum
    value = get_global_space_numeric_value "last_public_portnum"
    value ? value : Port::PUBLIC_PORTNUM_RESERVED
  end


  def get_last_private_portnum
    value = get_global_space_numeric_value "last_private_portnum"
    value ? value : Port::PRIVATE_PORTNUM_RESERVED
  end


  def set_last_public_portnum(portnum, txn=@db)
    set_global_space_value txn, "last_public_portnum", portnum
  end


  def set_last_private_portnum(portnum, txn=@db)
    set_global_space_value txn, "last_private_portnum", portnum
  end


  # A zero value is not a valid session ID.  There are no other constraints
  # on the structure or meaning of session IDs.  The values should be
  # pseudo random and drawn from a large space to prevent accidental
  # continuation of stale sessions.  However, the values need not be
  # strongly unpredictable or unguessable.
  #
  # WARNING: The session ID should NEVER be used for any authentication or
  #          cryptographic purposes.
  def generate_session_id
    # Error reporting is broken somewhere in either sqlite v3.5.6 or 
    # sqlite3-ruby v1.2.1.  An insert statement that violates a uniqueness
    # constraint causes SQLite3::SQLException rather than
    # SQLite3::ConstraintException to be raised.  Thus, we have to query
    # first and then insert second, rather than doing a combined insert-check
    # operation for prior existence of a session ID.
    loop do
      session_id = rand(281474976710656) + 1  # [1, 2**48]
      value = @db.first_value_from "SELECT session_id FROM AllocatedSessions
                                    WHERE session_id=? ", session_id
      if value
        $log.info "GlobalState#generate_session_id: session_id=%#x already " +
          "in use; choosing another", session_id
      else
        @db.execute "INSERT INTO AllocatedSessions VALUES(?)", session_id
        return session_id
      end

=begin
      begin
        @db.execute "INSERT INTO AllocatedSessions VALUES(?)", session_id
      rescue SQLite3::ConstraintException
        $log.info "GlobalState#generate_session_id: session_id=%#x already " +
          "in use; choosing another", session_id
      else
        return session_id
      end
=end
    end
  end


  def allocated_session_id?(session_id)
    value = @db.first_value_from "SELECT session_id FROM AllocatedSessions
                                  WHERE session_id=? ", session_id
    return value ? true : false
  end


  def start_checkpoint
    @db.transaction do |txn|
      @checkpoint_txn = txn
      @checkpoint_timestamp = Time.now.to_i

      txn.execute "INSERT INTO ChangeLog(id,timestamp,operation)
                   VALUES(NULL, ?, ?)", @checkpoint_timestamp, "checkpoint"

      @checkpoint_id = txn.last_insert_rowid
      $log.info "starting checkpoint: id=%d", @checkpoint_id
      begin
        yield txn, @checkpoint_id
        $log.info "finished checkpoint: id=%d", @checkpoint_id
      ensure
        @checkpoint_txn = nil
        @checkpoint_timestamp = nil
        @checkpoint_id = nil
      end
    end
  end


  def find_latest_checkpoint
    @db.first_row_from "SELECT id,timestamp FROM ChangeLog
                        WHERE id IN (SELECT max(id) FROM ChangeLog
                                     WHERE operation='checkpoint');"
  end


  private #-----------------------------------------------------------------

  def get_global_space_numeric_value(key)
    value = get_global_space_text_value key
    value ? value.to_i : nil
  end


  def get_global_space_text_value(key)
    @db.first_value_from("SELECT value FROM GlobalSpace WHERE key=? ", key)
  end


  def set_global_space_value(txn, key, value)
    txn.execute("INSERT OR REPLACE INTO GlobalSpace VALUES(?, ?)", key, value)
  end


  def open_state_database(path)
    retval = Amalgalite::Database.new path
    unless retval
      $log.err "ERROR: couldn't open/create state database file '%s'", path
      exit 1
    end

    #XXX retval.busy_timeout 100 # milliseconds to wait before retrying
    retval.busy_handler do |retries|
      if retries % 10 == 0
        $log.info "state db busy: tried %d times", retries
      end
      true
    end

    retval.execute_batch <<SCHEMA_EOF
CREATE TABLE IF NOT EXISTS GlobalSpace
(
  key          TEXT NOT NULL PRIMARY KEY,
  value        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS AllocatedSessions
(
  session_id   INTEGER NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS ChangeLog
(
  id           INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  timestamp    INTEGER NOT NULL,
  operation    TEXT NOT NULL,
  arg1         TEXT,
  arg2         TEXT,
  arg3         TEXT
);

CREATE TABLE IF NOT EXISTS Regions
(
  checkpoint_id    INTEGER NOT NULL,
  port             INTEGER NOT NULL,
  tuple_seqnum     INTEGER NOT NULL,
  node_id          INTEGER,
  session_id       INTEGER
);

CREATE INDEX IF NOT EXISTS regions_checkpoint_idx
  ON Regions(checkpoint_id);

CREATE TABLE IF NOT EXISTS RegionTuples
(
  checkpoint_id  INTEGER NOT NULL,
  port           INTEGER NOT NULL,
  seqnum         INTEGER NOT NULL,
  tuple          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS tuples_checkpoint_idx
  ON RegionTuples(checkpoint_id);

CREATE TABLE IF NOT EXISTS RegionTemplates
(
  checkpoint_id  INTEGER NOT NULL,
  port           INTEGER NOT NULL,
  seqnum         INTEGER NOT NULL,
  session_id     INTEGER NOT NULL,
  operation      TEXT NOT NULL,
  template       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS templates_checkpoint_idx
  ON RegionTemplates(checkpoint_id);

CREATE TABLE IF NOT EXISTS Connections
(
  checkpoint_id  INTEGER NOT NULL,
  node_id        INTEGER NOT NULL,
  session_id     INTEGER NOT NULL,
  protocol_version INTEGER,
  banner         TEXT,
  demux_seqnum   INTEGER NOT NULL,
  mux_seqnum     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS connections_checkpoint_idx
  ON Connections(checkpoint_id);

CREATE TABLE IF NOT EXISTS UnackedMessages
(
  checkpoint_id  INTEGER NOT NULL,
  session_id     INTEGER NOT NULL,
  seqnum         INTEGER NOT NULL,
  message        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_checkpoint_idx
  ON UnackedMessages(checkpoint_id);

CREATE TABLE IF NOT EXISTS OngoingRequests
(
  checkpoint_id   INTEGER NOT NULL,
  session_id      INTEGER NOT NULL,
  command_seqnum  INTEGER NOT NULL,
  operation       TEXT NOT NULL,
  template        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS requests_checkpoint_idx
   ON OngoingRequests(checkpoint_id);

SCHEMA_EOF
    retval
  end

end

end  # module Marinda
