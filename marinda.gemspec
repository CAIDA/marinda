#!/usr/bin/env ruby

# $Id: marinda.gemspec,v 1.8 2009/03/09 07:27:42 youngh Exp $

require 'rubygems'
require 'fileutils'

#---------------------------------------------------------------------------

MY_VERSION = "0.9.1"

MY_EXTRA_FILES = ["README", "CHANGES", "COPYING"]

candidates = Dir.glob("{bin,lib,test}/**/*")
candidates.concat Dir.glob("docs/*.{html,css,rb,txt,png}")
candidates.concat MY_EXTRA_FILES

MY_FILES = candidates.delete_if do |item|
  item.include?("CVS") || item.include?("rdoc") || item =~ /\~$/ ||
    File.directory?(item)
end

#---------------------------------------------------------------------------

spec = Gem::Specification.new do |s|
  s.name      = "marinda"
  s.version   = MY_VERSION
  s.author    = "Young Hyun"
  s.email     = "youngh@rubyforge.org"
  s.homepage  = "http://www.caida.org"
  s.platform  = Gem::Platform::RUBY
  s.summary   = "Distributed tuple space"
  s.description = <<-EOF
Distributed tuple space
EOF
  s.files     = MY_FILES
  s.require_path = "lib"
  #s.rubyforge_project = "marinda"
  #s.test_file = "test/ts_marinda.rb"
  s.has_rdoc  = false
  s.extra_rdoc_files = MY_EXTRA_FILES

  # Although adding this is nice in theory, it's annoying in practice, since
  # RubyGems will try to automatically upgrade amalgalite if one is already
  # installed, and the user may not want that.  At a minimum, RubyGems will
  # try to fetch the gem index.
  #s.add_dependency('amalgalite', '>= 0.12.0')
end

if $0 == __FILE__
  puts "Generating documentation"
  Dir.chdir("docs") do
    Dir.glob("*.txt") do |file|
      command = "asciidoc #{file}"
      puts "  " + command
      system command
    end
  end

  puts
  gem_name = "marinda-#{MY_VERSION}.gem"
  File.delete gem_name if File.exists? gem_name
  Gem::Builder.new(spec).build

  tar_dir = "marinda-#{MY_VERSION}"
  puts "\nBuilding dist directory " + tar_dir
  FileUtils.rm_r tar_dir if File.exists? tar_dir
  FileUtils.mkdir tar_dir
  candidates.each do |file|
    next if file =~ /^(lib|test)/
    dest_file = file.sub /^bin\//, ""
    dest_dir = File.dirname dest_file
    FileUtils.mkpath tar_dir + "/" + dest_dir unless dest_dir == "."
    dest_path = tar_dir + "/" + dest_file
    puts "  " + file + " => " + dest_path
    FileUtils.cp file, dest_path, :preserve => true
  end
  FileUtils.cp gem_name, tar_dir, :preserve => true

  tar_name = tar_dir + ".tar"
  tar_gz_name = tar_name + ".gz"
  File.delete tar_name if File.exists? tar_name
  File.delete tar_gz_name if File.exists? tar_gz_name
  puts "\nCreating " + tar_gz_name
  system "tar cvf #{tar_name} #{tar_dir}"
  system "gzip -9 #{tar_name}"
  system "ls -l #{tar_gz_name}"
end
