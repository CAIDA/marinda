#############################################################################
## Various utilities.
##
## --------------------------------------------------------------------------
## Copyright (C) 2009 The Regents of the University of California.
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

  # Returns a "handle" to a lock file opened with exclusive access, or nil on
  # error.  In some uncommon error scenarios, this method will raise an
  # exception (e.g., if a lock file already exists and is owned by someone
  # else).
  #
  # You should release the lock eventually by invoking release_exclusive_lock().
  # However, if your process dies while holding a lock, then the lock is
  # effectively released.
  def self.obtain_exclusive_lock(path)
    retval = nil
    num_tries = 0
    begin
      file_exists = false
      begin
        retval = File.open path, File::CREAT|File::EXCL|File::WRONLY, 0600
      rescue Errno::EEXIST
        file_exists = true
      end
      retval = File.open path, File::WRONLY, 0600 if file_exists
    rescue Errno::ENOENT  # handle a very rare race condition
      num_tries += 1
      raise if num_tries == 3
      retry 
    end

    unless retval
      $stderr.puts "ERROR: couldn't create/open lock file '#{path}'"
      return nil
    end

    if retval.flock File::LOCK_EX|File::LOCK_NB
      retval.truncate 0
      retval.puts $$
      retval.fsync
      retval.instance_variable_set :@___saved_lock_file_path, path
      return retval
    else
      retval.close
      $stderr.puts "ERROR: couldn't obtain exclusive lock on file '#{path}'; perhaps another process is holding the lock"
      return nil
    end
  end


  def self.release_exclusive_lock(lock)
    path = lock.instance_variable_get :@___saved_lock_file_path
    if path
      File.delete path
      lock.flock File::LOCK_UN
      lock.close
    end
  end


  # Based on perlipc(1).
  def self.daemonize
    Dir.chdir "/"
    STDIN.reopen "/dev/null", "r"
    STDOUT.reopen "/dev/null", "w"
    STDERR.reopen STDOUT
    $stdin = STDIN
    $stdout = STDOUT
    $stderr = STDERR
    fork && exit(0)
    Process.setsid
  end


  def self.sleep_at_least(sleep_amount)
    while sleep_amount > 0
      sleep_amount -= sleep sleep_amount
    end
  end

end  # module Marinda
