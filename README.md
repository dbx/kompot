# Kompot

Job queue and RPP engine for human use

## Features

This is a library used for inter-process communication through a Redis instance.

### Asynchronous messaging

Send asynchronous events to every client in an event group.

### Synchronous messaging

Send messages to any client that can process the given message type. Receive response in a future instance.

### Broadcast messaging

Send message to every client that can process the given broadcast type. Fast asynchronous method without guarantees and response.

# Development

## Building

Install Leiningen then execute `$ lein pom` to generate a `pom.xml` file. 
You can use the maven file to import the Java project into your favourite IDE.

## Documentation

Writing javadoc is a must. [This is how we do it.](http://www.dummies.com/programming/java/how-to-use-javadoc-to-document-your-classes/)

## Unit tests

- Use a local Redis instance to run unit tests.
- Write both positive and negative tests. Put emphasis on error handling.

# License

Licensed under the [Eclipse Public License](http://www.opensource.org/licenses/eclipse-1.0.php).
