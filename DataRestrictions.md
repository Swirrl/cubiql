#CubiQL RDF data restrictions 

This list contains the temporary CubiQL data restrictions and will be updated when they change. The RDF data should apply to these restrictions when running CubiQL against your own SPARQL end point.

CubiQL requires data to be modeled using the [RDF Data Cube Vocabulary](https://www.w3.org/TR/vocab-data-cube/). However, there are some more assumptions/restrictions for the data to be compatible with CubiQL:

- Literals should *not* have language tags (e.g. @en)
- Always use the qb:measureType even if there is only one measure.
- Reference area should be defined as: http://purl.org/linked-data/sdmx/2009/dimension#refArea
- Reference period should be defined as: http://purl.org/linked-data/sdmx/2009/dimension#refPeriod
- Time values should be expressed using reference.data.gov.uk e.g. http://reference.data.gov.uk/id/year/2016
- A qb:codeList should be defined for each dimension of the cube (except refArea and refPeriod) that contains *only* the values used at the cube 
- The code lists should be associated at the qb:ComponentSpecification (The QB vocabulary requires the code lists to be defined at the qb:DimensionProperty)
- The skos:Concepts of the code list should use rdfs:label (instead of skos:prefLabel)
- Data cubes should have at most one dcterms:license, dcterms:issued, dcterms:modified,  dcterms:publisher.

 