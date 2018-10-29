## CubiQL RDF data restrictions 

This list contains the CubiQL data restrictions. The RDF data should apply to these restrictions when running CubiQL against your own SPARQL end point.

CubiQL requires data to be modeled using the [RDF Data Cube Vocabulary](https://www.w3.org/TR/vocab-data-cube/). However, there are some more assumptions/restrictions for the data to be compatible with CubiQL:

- Multiple measures should be modeled using the measure dimension approach (i.e. use qb:measureType)
- Always use the qb:measureType even if there is only one measure.
- The codes used for each of the cube dimensions (except the geo and time dimensions) should be defined at a separate code list (e.g. skos:ConceptScheme). The code list should contain **only** the codes used.
- A code list should be defined also for the qb:measureType dimension
- This code list can be associated to either a qb:ComponentSpecification or a qb:DimensionProperty. 
- The predicate to associate this code list to the qb:ComponentSpecification or a qb:DimensionProperty can be defined at the configuration (it can be qb:codeList or any other property)
- Only URIs (NOT String or xsd:date) should be used for the values of the dimension (code lists cannot be defined based on strings)
- The geo dimension defined at the cofiguration should take values that have a label 
- The time dimension defined at the configuration should take values URIs defined by reference.data.gov.uk e.g. http://reference.data.gov.uk/id/year/2016
- If geo and/or time dimensions do not match the 2 above criteria then they should **not** be defined at the configuration and they will be handled like all the other dimensions 

Temporal requirements that will be fixed:
- Datasets, dimensions, measures and codelist members should all have a single `rdfs:label` with a language tag matching the configured `:schema-label-language` in the configuration (nil can be specified to use strings without an associated language tag). This value is used to generate elements of the GraphQL schema such as field names and enum values. Additional `rdfs:label`s with language tags can be defined, although only a single label should be defined for each element for a particular language. The requirement for string literal labels will be lifted when the GraphQL schema mapping is defined explicitly, see [#10](https://github.com/Swirrl/cubiql/issues/10) and 
  [#40](https://github.com/Swirrl/cubiql/issues/40).

 
