# encoding: utf-8
require File.expand_path("../lib/mongo-proxy.rb", __FILE__)

Gem::Specification.new do |gem|
  gem.add_runtime_dependency 'json'
  gem.add_runtime_dependency 'bson'
  gem.add_runtime_dependency 'bson_ext'
  gem.add_runtime_dependency 'em-proxy'
  gem.add_runtime_dependency 'trollop'

  gem.add_development_dependency 'mongo'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'rake-compiler'
  gem.add_development_dependency 'rspec'
  gem.add_development_dependency 'mocha'
  gem.add_development_dependency 'autotest-standalone'
  gem.add_development_dependency 'rack-test'

  gem.authors = ["Peter Bakkum"]
  gem.bindir = 'bin'
  gem.description = %q{A proxy server for MongoDB.}
  gem.email = ['pbb7c@virginia.edu']
  gem.executables = ['mongo-proxy']
  gem.extra_rdoc_files = ['LICENSE.md', 'README.md']
  gem.files = Dir['LICENSE.md', 'README.md', 'mongo-proxy.gemspec', 'Gemfile', '.rspec', '.travis.yml', 'lib/**/*', 'bin/*', 'spec/**/*']
  gem.homepage = 'http://github.com/bakks/mongo-proxy'
  gem.name = 'mongo-proxy'
  gem.rdoc_options = ["--charset=UTF-8"]
  gem.require_paths = ['lib']
  gem.required_rubygems_version = Gem::Requirement.new(">= 1.3.6")
  gem.summary = %q{A proxy server for MongoDB.}
  gem.test_files = Dir['spec/**/*']
  gem.version = MongoProxy::VERSION
  gem.license = 'MIT'
end

