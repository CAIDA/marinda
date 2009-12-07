#############################################################################
## TCP connections between the local and global servers.
##
## See Marinda::Client and Marinda::Channel for the classes that implement
## connections between clients and the local server.
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
##
## $Id: connection.rb,v 1.24 2009/03/17 00:54:19 youngh Exp $
#############################################################################

require 'socket'
require 'openssl'

# NOTE: Be sure to set BasicSocket.do_not_reverse_lookup to false at program
#       startup or IPSocket#peeraddr() might be slow.

module Marinda

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# A mixin for socket objects for tracking connection state in both the local
# and global servers.
#
# Because Kernel::select only works with IO objects, we need some way of
# determining what to do with an IO object marked as ready by select.
# We also need a way of finding the higher-level object associated with
# a given IO object.
module ConnectionState

  # Valid states:
  #
  #  :connecting = a local server is performing a connect_nonblock to the
  #               the global server (only applies to the local server)
  #
  #  :ssl_connecting = a local server completed connect_nonblock to the
  #               global server, and we now need to activate SSL with
  #               SSL#connect_nonblock (only applies to the local server)
  #
  #  :listening = server socket is listening for connections, and we need
  #               to complete accept_nonblock for each incoming connection
  #
  #  :ssl_accepting = global server socket completed accept_nonblock, and
  #               we now need to activate SSL with SSL#accept_nonblock (only
  #               applies to the global server)
  #
  #  :connected = a local/global server socket completed accept_nonblock
  #               (with or without SSL, according to server configuration),
  #               and we now have a client socket ready for reading/writing;
  #               or a connection from a local server to the global server
  #               is fully established (with or without SSL)
  #
  #  :defunct = socket disconnected (normally or abnormally), or a new
  #               connection to the global server displaced a previous
  #               connection from the same remote node
  #
  # Non-SSL client connections transition from :listening to :connected
  # (local and global servers).
  #
  # SSL client connections transition from :listening to :ssl_accepting to
  # :connected (global server only).
  #
  # An SSL connection from a local server to the global server transitions
  # from :connecting to :ssl_connecting to :connected.
  attr_accessor :__connection_state

  # The connection object associated with a given socket.
  #
  # This value is only really used when __connection_state == :connected,
  # even though this attribute is set in other states merely out of
  # completeness.  In the :connected state, this attribute is a SockState
  # object in the global server, and either a Channel or a GlobalSpaceMux
  # in the local server, depending on whether the connection is from a
  # client or the connection is to the global server, respectively.
  attr_accessor :__connection

end


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
module SSLSupport

  # NOTES:
  #
  #  * cert_file and key_file should be in PEM format, though others might
  #    be supported.
  #
  #  * If key_file is password protected, then you will be prompted for a
  #    password.  To remove the password protection, do
  #
  #       openssl rsa -in key.pem -out key.INSECURE.pem
  #       chmod 600 key.INSECURE.pem
  #
  #  * ca_file and ca_path should not both be defined; at least one should
  #    be defined.
  #
  #  * ca_file should be in PEM format, though others might be supported.
  #
  #  * ca_path should give the path to a directory containing individual
  #    CA certificate files with names that are generated by hashing the
  #    certificates in some special way, which the c_rehash script
  #    bundled with OpenSSL does for you.
  #
  # Note: This may raise OpenSSL::SSL::SSLError.
  def create_ssl_context(cert_file, key_file, ca_file, ca_path)
    cert = OpenSSL::X509::Certificate.new File::read(cert_file)
    key = OpenSSL::PKey::RSA.new File::read(key_file)

    retval = OpenSSL::SSL::SSLContext.new
    retval.cert = cert
    retval.key = key
    retval.verify_mode =
      OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
    retval.ca_file = ca_file if ca_file
    retval.ca_path = ca_path if ca_path

    # NOTE: The inclusion of OP_NO_TLSv1 is a (non-ideal) workaround for
    #       some bug in OpenSSL.  This option should be removed when the
    #       bug is finally squashed.  In short, without this option,
    #       connections to and from localhost fail with
    #
    #  client: OpenSSL::SSL::SSLError: sslv3 alert bad record mac
    #  server: OpenSSL::SSL::SSLError: decryption failed or bad record mac
    #
    #       Remote connections still work without this option, however.
    #       The issue seems to be related to OP_TLS_ROLLBACK_BUG
    #       (see SSL_CTX_set_options(3)), but using this option doesn't help.
    #       See also http://marc.theaimsgroup.com/?l=openssl-dev&m=108444932120941&w=2
    retval.options = OpenSSL::SSL::OP_NO_SSLv2 | OpenSSL::SSL::OP_NO_TLSv1
    retval
  end

end


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class InsecureClientConnection

 include Socket::Constants

  attr_reader :sock
  attr_accessor :need_io

  def initialize(host, port)
    @host = host
    @port = port
    @sock = nil
    @sockaddr = nil
    @need_io = :connect
  end


  # Usage:
  #
  #   Socket#connect_nonblock should raise IO::WaitWritable
  #   (Errno::EINPROGRESS) on the first call to indicate connect has
  #   started (SYN sent).  The caller should then wait for the socket to
  #   become writable and then try the connect_nonblock again.  Although
  #   not strictly necessary (the writable socket status by itself
  #   indicates either successful connection or a failure), the second call
  #   to connect_nonblock allows us to detect connection errors immediately
  #   rather than at the next read/write attempt.  If the connection
  #   succeeds on the second call, connect_nonblock raises Errno::EISCONN.
  #   Otherwise, connect_nonblock raises a connection error like
  #   Errno::ECONNREFUSED.
  #
  #   connect_nonblock should never simply return without raising an
  #   exception.
  def connect
    begin
      # Create the sockaddr here to catch DNS lookup failure.
      @sockaddr = Socket.sockaddr_in @port, @host unless @sockaddr
      unless @sock
        @sock = Socket.new AF_INET, SOCK_STREAM, 0
        @sock.extend ConnectionState
        @sock.__connection_state = :connecting
        # @sock.__connection not used; leave nil
      end

      @sock.connect_nonblock @sockaddr

      fail "INTERNAL ERROR: InsecureClientConnection#connect: " +
        "connect_nonblock didn't raise an exception"

    # not sure Errno::EINTR is possible
    rescue Errno::EINTR
      raise

    # Ruby 1.9.2 preview 2 uses IO::WaitWritable, but earlier versions use
    # plain Errno::EINPROGRESS, so technically we could get rid of
    # IO::WaitWritable.
    rescue Errno::EINPROGRESS, IO::WaitWritable
      @need_io = :write
      raise

    rescue Errno::EISCONN  # == connection established
      @need_io = :none
      @sock.__connection_state = :connected
      @sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true
      return @sock

    rescue
      $log.err "couldn't open connection to global server: %p", $!

      # Reset the attributes of this object so that the caller may simply
      # call #connect (after a delay) to retry connecting.
      reset()
      return nil
    end
  end


  def reset
    if @sock
      @sock.close rescue nil
      @sock.__connection_state = :defunct
      @sock = nil
    end
    @sockaddr = nil
    @need_io = :connect
  end

end


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Note: Set up the SSL context with ClientSSLConnection::set_ssl_context
#       prior to calling connect.
class ClientSSLConnection

  @@context = nil
  @@check_server_name = nil

  self.extend SSLSupport

  def self.set_ssl_context(cert_file, key_file, ca_file, ca_path,
                           check_server_name)
    @@context = create_ssl_context cert_file, key_file, ca_file, ca_path
    @@check_server_name = check_server_name
  end

  #........................................................................

  attr_accessor :need_io

  # {client_sock} is a socket returned by Socket#connect (that is, an
  # already established TCP connection).
  def initialize(host, client_sock)
    @host = host
    @client_sock = client_sock
    @client_sock.__connection_state = :ssl_connecting
    @client_sock.__connection = self

    @ssl = nil  # SSLSocket wrapping @client_sock
    @need_io = :write
  end


  def sock
    @client_sock
  end


  # This passes out IO::WaitReadable or IO::WaitWritable if
  # SSLSocket#connect_nonblock would block.
  def connect
    begin
      unless @ssl
        @ssl = OpenSSL::SSL::SSLSocket.new @client_sock, @@context
        @ssl.sync_close = true
      end

      @ssl.connect_nonblock
      @ssl.extend ConnectionState
      @ssl.__connection_state = :connected
      @ssl.__connection = nil  # will be set later by caller
      @ssl.post_connection_check @host if @@check_server_name
      return @ssl

    # not sure Errno::EINTR is possible with SSLSocket#connect_nonblock
    rescue Errno::EINTR
      raise

    rescue IO::WaitReadable
      @need_io = :read
      raise

    rescue IO::WaitWritable
      @need_io = :write
      raise

    # post_connection_check can raise OpenSSL::SSL::SSLError.
    rescue
      $log.err "couldn't establish SSL with global server: %p", $!
      @ssl.close rescue nil if @ssl
      @ssl = nil
      @client_sock.close rescue nil
      @client_sock.__connection_state = :defunct
      @client_sock = nil
      return nil
    end

  end

end


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class InsecureServerConnection

  attr_reader :sock

  # Note: This may raise a SystemCallError.
  def initialize(port)
    @port = port

    # NOTE: The use of '0.0.0.0' is a workaround for a problem on FreeBSD.
    #       Without it, TCPServer creates an IPv6 listening socket even
    #       on machines without IPv6 connectivity.  Moreover, this IPv6
    #       socket only accepts IPv6 packets, unlike on some systems (e.g.,
    #       MacOS X) in which IPv6 sockets can (in certain situations)
    #       accept connections from both IPv4 and IPv6 clients.
    #
    #       TCPServer.new choosing an IPv6 socket rather than an IPv4
    #       socket on a system without IPv6 connectivity is an issue,
    #       perhaps, with Ruby.  However, the fact that IPv6 sockets don't
    #       support IPv4 clients on FreeBSD systems is an issue with
    #       FreeBSD.  Indeed, FreeBSD intentionally disallows this
    #       behavior by default, presumably in accordance with the
    #       recommendations of the Internet draft "IPv4-Mapped Addresses
    #       on the Wire Considered Harmful"
    #   (http://tools.ietf.org/html/draft-itojun-v6ops-v4mapped-harmful-02)
    #
    #       This feature is controlled by net.inet6.ip6.v6only, which is
    #       true by default (verified under FreeBSD 5.4 and 6.1).
    #       This feature can also be toggled with the IPV6CTL_V6ONLY
    #       socket option (at level IPPROTO_IPV6), but Ruby doesn't provide
    #       a symbolic constant for this option (so toggling it under
    #       Ruby is non-portable).
    #
    #       See IP6(4) and inet6(4) on FreeBSD systems for more details.
    @sock = TCPServer.new '0.0.0.0', @port
    @sock.extend ConnectionState
    @sock.__connection_state = :listening
    @sock.__connection = self
  end


  def accept
    begin
      retval = @sock.accept_nonblock
      retval.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true

      retval.extend ConnectionState
      retval.__connection_state = :connected
      retval.__connection = nil  # will be set later by caller
      retval
    rescue Errno::EWOULDBLOCK, Errno::EINTR  # not sure EINTR is raised
      # The global server always retries, so no need to pass out an exception
      # telling it to retry.
      return nil
    rescue
      $log.err "InsecureServerConnection#accept failed: %p", $!
      return nil
    end
  end


  # This implements a simple firewall by only accepting connections from the
  # known IP addresses of nodes.
  def accept_with_whitelist(nodes)
    client_sock = nil
    begin
      client_sock = @sock.accept_nonblock
      client_sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true

      client_sock.extend ConnectionState
      client_sock.__connection_state = :connected
      client_sock.__connection = nil  # will be set later by caller

      # Note: peeraddr() can raise Errno::EINVAL if the remote end closes
      #       the socket (see getpeername(2)).
      peer_ip = client_sock.peeraddr[3]
      node_id = nodes[peer_ip]
      if node_id
        $log.info "accepting connection from whitelisted %s, node %d",
          peer_ip, node_id
        return client_sock, peer_ip, node_id
      else
        $log.notice "rejecting connection from %s: node not in whitelist",
          peer_ip
        client_sock.close rescue nil
        return nil
      end

    rescue Errno::EWOULDBLOCK, Errno::EINTR  # not sure EINTR is raised
      # The global server always retries, so no need to pass out an exception
      # telling it to retry.
      return nil

    rescue
      $log.err "couldn't perform accept() on server socket: %p", $!
      client_sock.close rescue nil if client_sock
      return nil
    end
  end

end


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Note: Set up the SSL context with AcceptingSSLConnection::set_ssl_context
#       prior to calling accept.
class AcceptingSSLConnection

  @@context = nil
  @@check_client_name = nil

  self.extend SSLSupport

  def self.set_ssl_context(cert_file, key_file, ca_file, ca_path,
                           check_client_name)
    @@context = create_ssl_context cert_file, key_file, ca_file, ca_path
    @@check_client_name = check_client_name
  end

  #........................................................................

  attr_accessor :need_io
  attr_reader :node_id

  # {client_sock} is a socket returned by TCPServer#accept_nonblock
  # (that is, an already established TCP connection).
  def initialize(client_sock, peer_ip, node_id)
    @need_io = :read
    @client_sock = client_sock
    @client_sock.__connection_state = :ssl_accepting
    @client_sock.__connection = self

    @peer_ip = peer_ip
    @node_id = node_id
    @ssl = nil  # SSLSocket wrapping @client_sock
  end


  def sock
    @client_sock
  end


  # This passes out IO::WaitReadable or IO::WaitWritable if
  # SSLSocket#accept_nonblock would block.
  def accept
    begin
      unless @ssl
        @ssl = OpenSSL::SSL::SSLSocket.new @client_sock, @@context
        @ssl.sync_close = true
      end

      @ssl.accept_nonblock
      @ssl.extend ConnectionState
      @ssl.__connection_state = :connected
      @ssl.__connection = nil  # will be set later by caller
      @ssl.post_connection_check @peer_ip if @@check_client_name
      return @ssl, @node_id

    # not sure Errno::EINTR is possible with SSLSocket#accept_nonblock
    rescue Errno::EINTR
      raise

    rescue IO::WaitReadable
      @need_io = :read
      raise

    rescue IO::WaitWritable
      @need_io = :write
      raise

    # post_connection_check can raise OpenSSL::SSL::SSLError.
    rescue
      $log.err "couldn't establish SSL with client: %p", $!
      @ssl.close rescue nil if @ssl
      @ssl = nil
      @client_sock.close rescue nil
      @client_sock.__connection_state = :defunct
      return nil
    end
  end

end

end  # module Marinda
