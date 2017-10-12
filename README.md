# graphql-qb

A proof of concept [GraphQL](http://graphql.org/) service for
querying [Linked Data Cubes](https://www.w3.org/TR/vocab-data-cube/)
that was produced as part of
the [Open Gov Intelligence](http://www.opengovintelligence.eu/)
project.

The primary aim of graphql-qb is to facilitate the querying of
multidimensional QB datasets through GraphQL in an easier more familiar
way than through SPARQL.

## Example

We have hosted an example graphql-qb service
at
[graphql-qb.publishmydata.com](http://graphql-qb.publishmydata.com/)
which is currently using data from 
from [statistics.gov.scot](http://statistics.gov.scot/).

### Example Queries

- Dataset Discovery
  - [List all datasets](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20datasets%20%7B%0A%20%20%20%20uri%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%7D%20%0A%7D)
  - [Find dataset by URI](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20datasets(uri%3A"http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fearnings")%20%7B%0A%20%20%20%20description%0A%20%20%20%20schema%0A%20%20%20%20title%0A%20%20%20%20uri%0A%20%20%7D%0A%7D)
- Dataset metadata
  - [Obtaining Dimension / Dimension Values](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20dimensions%20%7B%0A%20%20%20%20%20%20uri%0A%20%20%20%20%20%20values%20%7B%0A%20%20%20%20%20%20%20%20label%0A%20%20%20%20%20%20%20%20uri%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)
- Filtering
  - [By Dimension (boolean or)](http://graphql-qb.publishmydata.com/index.html?query={%0A%20datasets(dimensions%3A%20{or%3A%20[%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2Fgender%22%2C%20%0A%20%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2FpopulationGroup%22]})%20{%0A%20title%0A%20description%0A%20uri%0A%20}%0A}%0A)
  - [By Dimension (boolean and)](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20datasets(dimensions%3A%7Band%3A%5B%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2Fgender%22%20%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2Fage%22%5D%7D)%20%7B%0A%20%20uri%0A%20%20title%0A%20%20description%0A%20%7D%0A%7D%0A)
  - [Filtering datasets about gender](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20datasets(dimensions%3A%7Band%3A%5B%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2Fgender%22%5D%7D)%20%7B%0A%20%20%20%20uri%0A%20%20%20%20title%0A%20%20%7D%0A%7D) 
  - [Filtering datasets about population group](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20datasets(dimensions%3A%7Band%3A%5B%22http%3A%2F%2Fstatistics.gov.scot%2Fdef%2Fdimension%2FpopulationGroup%22%5D%7D)%20%7B%0A%20%20%20%20uri%0A%20%20%20%20title%0A%20%20%7D%0A%7D)

- Locking Dimensions
  - [and counting matches](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%7Bgender%3AALL%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3AWORKPLACE_BASED%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3AMEDIAN%7D)%20%7B%0A%0A%20%20%20%20%20%20total_matches%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)
  - [and getting observations](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%7Bgender%3AALL%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3AWORKPLACE_BASED%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3AMEDIAN%7D)%20%7B%0A%0A%20%20%20%20%0A%20%20%20%20%20%20page%20%7B%0A%20%20%20%20%20%20%20%20result%20%7B%0A%20%20%20%20%20%20%20%20%20%20gender%0A%20%20%20%20%20%20%20%20%20%20measure_type%0A%20%20%20%20%20%20%20%20%20%20population_group%0A%20%20%20%20%20%20%20%20%20%20reference_area%0A%20%20%20%20%20%20%20%20%20%20reference_period%0A%20%20%20%20%20%20%20%20%20%20uri%0A%20%20%20%20%20%20%20%20%20%20median%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)

- Aggregations (scoped to current filters/locks)
  - [average](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%20%7Bgender%3A%20ALL%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3A%20WORKPLACE_BASED%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3A%20MEDIAN%7D)%20%7B%0A%20%20%20%20%20%20aggregations%20%7B%0A%20%20%20%20%20%20%20%20average(measure%3A%20MEDIAN)%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)
  - [max](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%20%7Bgender%3A%20ALL%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3A%20WORKPLACE_BASED%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3A%20MEDIAN%7D)%20%7B%0A%20%20%20%20%20%20aggregations%20%7B%0A%20%20%20%20%20%20%20%20max(measure%3A%20MEDIAN)%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)
  - [min](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%20%7Bgender%3A%20ALL%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3A%20WORKPLACE_BASED%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3A%20MEDIAN%7D)%20%7B%0A%20%20%20%20%20%20aggregations%20%7B%0A%20%20%20%20%20%20%20%20min(measure%3A%20MEDIAN)%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)
  - [sum](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20title%0A%20%20%20%20description%0A%20%20%20%20observations(dimensions%3A%20%7Bgender%3A%20ALL%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3A%20WORKPLACE_BASED%2C%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3A%20MEDIAN%7D)%20%7B%0A%20%20%20%20%20%20aggregations%20%7B%0A%20%20%20%20%20%20%20%20sum(measure%3AMEDIAN)%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)

- Misc
  - [Getting SPARQL query](http://graphql-qb.publishmydata.com/index.html?query=%7B%0A%20%20dataset_earnings%20%7B%0A%20%20%20%20observations(dimensions%3A%7Bgender%3AALL%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20population_group%3AWORKPLACE_BASED%20%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20measure_type%3AMEDIAN%7D)%20%7B%0A%20%20%20%20%20%20sparql%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D)
  - [Parameterised query](http://graphql-qb.publishmydata.com/index.html?query=query%20datasetsQuery(%24dataset%3A%20uri)%20%7B%0A%20%20datasets(uri%3A%20%24dataset)%20%7B%0A%20%20%20%20title%0A%20%20%20%20uri%0A%20%20%7D%0A%7D&variables=%7B%22dataset%22%3A%20%22http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fearnings%22%7D%0A)

### Using graphql voyager 

You can browse our schema by following these steps:

1. [Run the graphql voyager introspection query on our endpoint](http://graphql-qb.publishmydata.com/index.html?query=%0A%20%20query%20IntrospectionQuery%20%7B%0A%20%20%20%20__schema%20%7B%0A%20%20%20%20%20%20queryType%20%7B%20name%20%7D%0A%20%20%20%20%20%20mutationType%20%7B%20name%20%7D%0A%20%20%20%20%20%20subscriptionType%20%7B%20name%20%7D%0A%20%20%20%20%20%20types%20%7B%0A%20%20%20%20%20%20%20%20...FullType%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20directives%20%7B%0A%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20description%0A%20%20%20%20%20%20%20%20locations%0A%20%20%20%20%20%20%20%20args%20%7B%0A%20%20%20%20%20%20%20%20%20%20...InputValue%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%0A%20%20fragment%20FullType%20on%20__Type%20%7B%0A%20%20%20%20kind%0A%20%20%20%20name%0A%20%20%20%20description%0A%20%20%20%20fields(includeDeprecated%3A%20true)%20%7B%0A%20%20%20%20%20%20name%0A%20%20%20%20%20%20description%0A%20%20%20%20%20%20args%20%7B%0A%20%20%20%20%20%20%20%20...InputValue%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20type%20%7B%0A%20%20%20%20%20%20%20%20...TypeRef%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20isDeprecated%0A%20%20%20%20%20%20deprecationReason%0A%20%20%20%20%7D%0A%20%20%20%20inputFields%20%7B%0A%20%20%20%20%20%20...InputValue%0A%20%20%20%20%7D%0A%20%20%20%20interfaces%20%7B%0A%20%20%20%20%20%20...TypeRef%0A%20%20%20%20%7D%0A%20%20%20%20enumValues(includeDeprecated%3A%20true)%20%7B%0A%20%20%20%20%20%20name%0A%20%20%20%20%20%20description%0A%20%20%20%20%20%20isDeprecated%0A%20%20%20%20%20%20deprecationReason%0A%20%20%20%20%7D%0A%20%20%20%20possibleTypes%20%7B%0A%20%20%20%20%20%20...TypeRef%0A%20%20%20%20%7D%0A%20%20%7D%0A%0A%20%20fragment%20InputValue%20on%20__InputValue%20%7B%0A%20%20%20%20name%0A%20%20%20%20description%0A%20%20%20%20type%20%7B%20...TypeRef%20%7D%0A%20%20%20%20defaultValue%0A%20%20%7D%0A%0A%20%20fragment%20TypeRef%20on%20__Type%20%7B%0A%20%20%20%20kind%0A%20%20%20%20name%0A%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20kind%0A%20%20%20%20%20%20name%0A%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20ofType%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20kind%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A)
2. Copy the result of the above query to your clipboard
3. Visit https://apis.guru/graphql-voyager/
4. Select custom schema
5. Paste the schema into the text area
6. Click change schema.

You'll see something like:
![screen shot 2017-09-08 at 15 48 29](https://user-images.githubusercontent.com/49898/30217232-6fcdf16e-94ad-11e7-9f65-3eaaa6ae0a5d.png)

## Running yourself

    $ java -jar graphql-qb-0.1.0-standalone.jar OPTIONS

The available options are:

|  Name    | Default |
|----------|---------|
| port     | 8080    |
| endpoint |         |

For example to run the server against a remote SPARQL endpoint on port 9000:

    $ java -jar graphql-qb-standalone.jar --port 9000 --endpoint http://remote-endpoint/sparql/query


If the endpoint is not specified the built-in test data will be used.

The server hosts a GraphQL endpoint at http://localhost:PORT/graphql which follows the
protocol described [here](http://graphql.org/learn/serving-over-http/).

## License

Copyright © 2017 Swirrl IT Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
