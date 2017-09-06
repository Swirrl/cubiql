# clj-graphql

Service which hosts a GraphQL endpoint with a schema generated from an RDF dataset definition.

## Usage

    $ java -jar clj-graphql-0.1.0-standalone.jar [PORT]

If not specified the server will be hosted on the default port 8080.

The server hosts a GraphQL endpoint at http://localhost:PORT/graphql which follows the
protocol described [here](http://graphql.org/learn/serving-over-http/).

## License

Copyright © 2017 Swirrl IT Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
