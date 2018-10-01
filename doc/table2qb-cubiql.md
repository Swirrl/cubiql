# Table2qb - CubiQL End-to-end example

Steps:
1. Create RDF ttl files using Table2qb
2. (Optional) Load the RDF files into a triple store
3. Run CubiQL against the generated RDF data.

## Step 1: Table2qb

[table2qb](https://github.com/Swirrl/table2qb/) is a command-line tool for generating RDF data cubes from tidy CSV data. See the `table2qb` repository for
installation instructions and an [example](https://github.com/Swirrl/table2qb/blob/master/examples/employment/README.md) of using it to generate data cubes. 

### Requirements

- Java 8
- Unix, Linux, Mac, Windows  environments
- On Windows run `table2qb` with `java -jar table2qb.jar exec ...`

### How to use

**Create the codelists using the `codelist-pipeline`**

Example:

`table2qb exec codelist-pipeline --codelist-csv csv/gender.csv --codelist-name "Gender" --codelist-slug "gender" --base-uri http://statistics.gov.scot/ --output-file ttl/gender.ttl`

- Codelists should exist for every dimension. If codelists currently exists there is no need to create new ones.
- A separate CSV is required for each codelist.
- The codelist CSV files should contain all the possibles values for each codelist
- An example CSV is: [gender.csv](https://github.com/Swirrl/table2qb/blob/master/examples/employment/csv/gender.csv)
- At the CSV use the column `Parent Notation` to define hierarchies
- The URIs created by the codelist pipeline
  - Concept scheme (codelist): `{base-uri}/def/concept-scheme/{codelist-slug}`
  - Concepts: `{base-uri}/def/concept/{codelist-slug}/{CSV_column_ Notation}`

**Create the cube components using the `components-pipeline`**

Example:
`table2qb exec components-pipeline --input-csv csv/components.csv --base-uri http://statistics.gov.scot/ --output-file ttl/components.ttl`

- The cube components are the `qb:DimensionProperty`, `qb:MeasureProperty` and `qb:AttributeProperty`.
- At the CSV use one row for each of the cube components. If some of the components have already been created (e.g. for another cube) do not include them in the CSV. 
- At the CSV column `Component Type` the types are: `Dimension`, `Measure` and `Attribute`
- At the CSV column `Codelist` use the URIs created by the `codelist-pipeline`
- An example CSV is: [components.csv](https://github.com/Swirrl/table2qb/blob/master/examples/employment/csv/components.csv)
- The URIs created by the `components-pipeline`:
  - Dimension: `{base-uri}/def/dimension/{CSV_column_ Label}`
  - Measure: `{base-uri}/def/measure/{CSV_column_ Label}`
  - Attribute: `{base-uri}/def/attribute/{CSV_column_ Label}`

**Create the cube DSD and observations using the `cube-pipeline`**

Example:
`table2qb exec cube-pipeline --input-csv csv/input.csv --dataset-name "Employment" --dataset-slug "employment" --column-config csv/columns.csv --base-uri http://statistics.gov.scot/ --output-file ttl/cube.ttl`

- Use a CSV file to define the mappings between a CSV column and the relevant cube component. 
- The CSV should contain one row per component i.e. `qb:DimensionProperty`, `qb:MeasureProperty` and `qb:AttributeProperty`. Including a row for the `qb:measureType`. It should also contain a row for the observation `value`.
- At the CSV, the column `component_attachment` can be: `qb:dimension`, `qb:measure`, `qb:attribute`. 
- At the CSV, the column `property_template` should match with the URIs created for the components by the `components-pipeline`. Use the csv column `name` if required at the template (e.g. `http://example.gr/def/measure/{measure_type}`)
- At the CSV, the column `value_template` should match with the URIs created for the concepts by the `codelist-pipeline`. Use the csv column `name` if required at the template (e.g. `http://example.gr/def/concept/stationid/{station_id}`)
- If there are numbers at the dimension values (not at the value of the observation) use `datatype` `string`. Otherwise if `datatype` `number` is used then the URIs will have the form e.g. `http://example.gr/def/condept/year/2000.0`
- At the CSV row that has the mapping for the measure (i.e. `component_attachment` `qb:measure`), leave the `value_template` empty.
- At the CSV row for the value (i.e. with `name` `value`). Leave the `component_attachment` and `value_template` empty.
- At each CSV row use a `value_transformation` if required. Possible values are: `slugize`, `unitize` or blank. `slugize` converts column values into URI components, e.g. `(slugize "Some column value")` is `some-column-value`. `unitize` translates literal `£` into `GBP`, e.g. `(unitize "£ 100") is `gpb-100`. **Be careful*: the `slugize` converts all to non-capital letters. The URIs of the dimension values should match with the the concept URIs created through the `codelist-pipeline`

**Advice:** 
- Use the same base URI at all pipelines. Although it is not mandatory it will easy the transformation process.
- **Be careful** to use URIs that match between the pipelines. E.g. the `property_template` URI at `cube-pipeline` should match with the URIs created for the components by the `components-pipeline`.

A complete example can be found at [Github](https://github.com/Swirrl/table2qb/tree/master/examples/employment).

## Step 2: (Optional) Load RDF to triple store

CubiQL can be run directly against a local directory containing the generated RDF data, however this loads the data into memory so is unsuitable for large amounts of data.
First loading the data into a dedicated triple store is therefore recommended.  

Supported triple stores:
- Virtuoso
- Stardog
- Fuseki ?

What to load at the triple store:
- All the RDF ttl created by Table2qb:
  - RDF for each of the codelists
  - RDF for the cube components
  - RDF for the DSD and the observations
- RDF for the [QB vocabulary](https://raw.githubusercontent.com/UKGovLD/publishing-statistical-data/master/specs/src/main/vocab/cube.ttl)

## Step 3: CubiQL

Run CubiQL using the default configuration:

`java -jar graphql-qb-0.4.0-standalone.jar --port 8085 --endpoint http://myendpoint.com`

if running against a local directory containing the data the `--endpoint` parameter should specify the path to the directory:

`java -jar graphql-qb-0.4.0-standalone.jar --port 8085 --endpoint ./ttl`

The default configuration:
```
{:geo-dimension-uri nil
 :time-dimension-uri nil
 :codelist-source "component"
 :codelist-predicate "http://publishmydata.com/def/qb/codesUsed"
 :codelist-label-uri "http://www.w3.org/2000/01/rdf-schema#label"
 :dataset-label-uri "http://www.w3.org/2000/01/rdf-schema#label"
 :schema-label-language nil
 :max-observations-page-size 2000}
```

If default configuration does not match your data, then use another configuration file:

`java -jar graphql-qb-0.4.0-standalone.jar --port 8085 --endpoint http://myendpoint.com/ --configuration myconfig.edn`

Configuration parameters:

- `:geo-dimension-uri` defines the geo dimension. The values of the geo dimension should have a labe. 
- `:time-dimension-uri` defines the time dimension. The values of the time dimensions should be defined by the `reference.data.gov.uk` e.g. `http://reference.data.gov.uk/id/year/2016`
- `:codelist-source` defines the source of the codelist that contains only the values used at the cube. The source can be: (i) `"component"` or (ii) `"dimension"`. By default Table2qb uses the `"component"`.
- `:codelist-predicate` defines the predicate that connects the `:codelist-source` with the codelist that  contains only the values used at the cube. By default Table2qb uses: `http://publishmydata.com/def/qb/codesUsed`. 
- `:codelist-label-uri` defines the label property that is used at the codelists. By default Table2qb uses: `http://www.w3.org/2000/01/rdf-schema#label `
- `:dataset-label-uri` defines the label property that is used at the dataset i.e. cube, DSD, components. By default Table2qb uses: `http://www.w3.org/2000/01/rdf-schema#label`
- Datasets, dimensions, measures and codelist members should all have a label with a language tag matching the `:schema-label-language`.  `nil` can be specified to use strings without an associated language tag.
- `:max-observations-page-size` defines the maximum page size e.g. if you need to get all the observations with one query.
