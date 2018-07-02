#!/bin/env ruby

MVN_HOST='dbx.services'

puts (`mvn clean package -DskipTests` or abort("Could not make package!"))

JARFILE = Dir.glob("target/kompot-*.jar").first
POMFILE = "pom.xml"

abort('Missing jar file, build it first!') unless JARFILE

GROUP = "hu/dbx"
ARTIFACT = "kompot"
VERSION = /kompot-(.*).jar/.match(JARFILE)[1]

puts "Csoport: #{GROUP}"
puts "Artifakt: #{ARTIFACT}"
puts "Verzio: #{VERSION}"

REMOTE_DIRECTORY="/home/ubuntu/maven/repository/#{GROUP}/#{ARTIFACT}/#{VERSION}"
puts (`ssh #{MVN_HOST} mkdir -p #{REMOTE_DIRECTORY}` or abort("Could not create dir"))

REMOTE_JAR_FILE="#{REMOTE_DIRECTORY}/stencil-#{VERSION}.jar"
puts (`scp #{JARFILE} #{MVN_HOST}:#{REMOTE_JAR_FILE}` or abort("Could not copy JAR!"))

REMOTE_POM_FILE="#{REMOTE_DIRECTORY}/stencil-#{VERSION}.pom"
puts (`scp #{POMFILE} #{MVN_HOST}:#{REMOTE_POM_FILE}` or abort("Could not copy POM!"))

puts "Good!"
