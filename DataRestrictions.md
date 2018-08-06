## CubiQL RDF data restrictions 

This list contains the CubiQL data restrictions. The RDF data should apply to these restrictions when running CubiQL against your own SPARQL end point.

CubiQL requires data to be modeled using the [RDF Data Cube Vocabulary](https://www.w3.org/TR/vocab-data-cube/). However, there are some more assumptions/restrictions for the data to be compatible with CubiQL:

- Multiple measures should be modeled using the measure dimension approach (i.e. use qb:measureType)
- Always use the qb:measureType even if there is only one measure.
- A qb:codeList should be defined for each dimension of the cube (except the geo and time dimensions) that contains *only* the values used in the cube 
- The geo dimension defined at the cofiguration should take values URIs that have a label 
- The time dimension defined at the configuration should take values URIs defined by reference.data.gov.uk e.g. http://reference.data.gov.uk/id/year/2016
- If geo and/or time dimensions do not match the 2 above criteria then they should **not** be defined at the configuration and they will be handled like all the other dimensions (issue [#108](https://github.com/Swirrl/graphql-qb/issues/108) should be fixed to allow NULL values for geo and time dimensions at the configuration)

Temporal requirements that will be fixed:
- Literals should **not** have language tags (e.g. @en) (this will be fixed by [#6](https://github.com/Swirrl/graphql-qb/issues/6))
- Data cubes should have at most one dcterms:license, dcterms:issued, dcterms:modified,  dcterms:publisher (this will be fixed by [#96](https://github.com/Swirrl/graphql-qb/issues/96))
- Datasets, dimensions, measures and codelist members should all have a single `rdfs:label` with a language tag matching the configured `:schema-label-language` in the configuration (nil can
  be specified to use strings without an associated language tag). This value is used to generate elements of the GraphQL schema such as field names and enum values. 
  Additional `rdfs:label`s with language tags can be defined, although only a single label should be defined for each element for a particular language. 
  The requirement for string literal labels will be lifted when the GraphQL schema mapping is defined explicitly, see [#10](https://github.com/Swirrl/graphql-qb/issues/10) and 
  [#40](https://github.com/Swirrl/graphql-qb/issues/40).

 
