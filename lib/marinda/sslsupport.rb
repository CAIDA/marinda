#############################################################################
## SSL-support utilities.
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
## $Id: $
#############################################################################

require 'openssl'

module Marinda

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

end  # module Marinda
