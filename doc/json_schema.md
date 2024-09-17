## <a>JSON Report Structure</a>

```json
{
  "type": 2,
  "datasets": {...},
  "sections": [...],
  "properties": {...}
}
```
* `type`: `1` - regular report, `2` - differencial report
* `datasets`: contains data sets for the report
* `sections`: contains a description (metadata) of all sections of the report, tables, their columns, graphs, as well as additional data for their construction
* `properties`: properties of the generated report (f.e. `topn`, `report_start1`, `max_query_length`)

### <a id='datasets'>Datasets</a>
JSON report has [`datasets`](#ajson-report-structurea) attribute in which report stores all
statistics data for the reporting time period. This data provided in number of datasets,
which store each value by a unique key. For example:
```json
{
  "type": 2,
  "datasets": {
    "dbstats": [
      {
        "datid": "17519",
        "dbname": "bench",
        "ord_db": 1,
        "datsize1": "143 MB",
        "datsize2": "143 MB",
        "blks_hit1": 7811421,
        "blks_hit2": 49030,
        ...
      },
      {
        "datid": "24020",
        "dbname": "demo",
        "ord_db": 2,
        "datsize1": "7564 kB",
        "datsize2": "7564 kB",
        "blks_hit1": 72951,
        "blks_hit2": 70063,
        ...
      }
    ]
  },
  "sections": [...],
  "properties": {...}
}
```

### <a id='sections'>Sections<a/>

```json
{
  ...
  "sections": [
    {
      "header": [...],
      "sect_id": "rep_details",
      "tbl_cap": "Report details",
      "toc_cap": "Report details",
      "content": {...}
    },
    {
      "sect_id": "srvstat",
      "tbl_cap": "Server statistics",
      "toc_cap": "Server statistics",
      "sections": [...]
    },
    {
      "header": [...],
      "content": {...},
      "sect_id": "iostatrst"
    }
  ]
}
```
`sections` is an ordered array that stores descriptions and other data for report sections.
May have next attributes:

* `sect_id`: Section identifier
* `tbl_cap`: Section title
* `toc_cap`: Section title in table of contents
* `header`: *(Optional)* If the section should contain a table this attribute contains a
description of the header of this table, which columns and what data it contains.
* `sections`: *(Optional)* The attribute shows whether the section has nested sections.
Usually, if a section consists of several tables (not just one), then each table is
designed as a separate section, in which case it does not have the `section` attribute
and has the `header` attribute.
* `content`: *(Optional)* Additional field with warning or notice

### <a>Content</a>
```json
{
  ...
  "sections": [
    {
      "header": [...],
      "content": {
        "text": "IO stats reset was detected during report interval. Statistic values may be affected",
        "class": "warning"
      },
      "sect_id": "iostatrst"
    }
  ]
}
```
Additional field with warning or notice with next attributes:

* `text`: Text of message
* `class`: Level of message. Can take two values: `warning` or `notice`

### <a>Header</a>

```json
{
  ...
  "sections": [
    {
      "header": [
        {
          "type": "row_table",
          "limit": "topn",
          "filter": {...},
          "scroll": [...],
          "source": "act_top_states",
          "columns": [...],
          "preview": [...],
          "ordering": "ord_dur",
          "highlight": [...]
        }
      ],
      "sect_id": "act_ix",
      "tbl_cap": "Top 'idle in transaction' session states by duration",
      "toc_cap": "Top 'idle in transaction' states"
    }
  ]
}
```

* `type`: Type of section. Could be `row_table` (regular table), `column_table` (vertical table) or `chart`.
* `columns`: *(Optional)* Describes the structure of header's [columns](#columns). Attribute `columns` may be absent only
when `type` is `chart`
* `filter`: *(Optional)* This attribute is using for [filtering](#filter) feature
* `scroll`: *(Optional)* Contains arrays of keys from [`datasets`](#datasets) objects. This array defines which values
in dataset should be used to generate unique id for each row in this html table (to create anchor link)
* `source`: *(Optional)* This attribute identifies which dataset in [`datasets`](#datasets) should be used to
build this `section`
* `preview`: *(Optional)* Contains data for [preview](#preview) feature
* `ordering`: *(Optional)* Contains name of attribute for data sorting. F.e. `ordering: ord_dur` means data
should be sorted by `ord_dur` attribute and `ordering: -ord_dur` means data should be sorted by `ord_dur`
attribute in descending order
* `limit`: *(Optional)* Maximum amount of rows. It overwrites default `topn` parameter. Limitation applies on dataset
only after `ordering` is done
* `highlight`: *(Optional)* Contains data for [highlight](#highlight) feature

### <a id='columns'>Columns</a>

```json
{
  ...
  "sections": [
    {
      "header": [
        {
          "type": "row_table",
          "limit": "topn",
          "filter": {...},
          "scroll": [...],
          "source": "act_top_states",
          "columns": [
            ...
            {
              "id": "application_name",
              "class": "table_obj_name",
              "title": "application_name",
              "caption": "App"
            },
            ...
          ],
          "preview": [...],
          "ordering": "ord_dur",
          "highlight": [...]
        }
      ],
      "sect_id": "act_ix",
      "tbl_cap": "Top 'idle in transaction' session states by duration",
      "toc_cap": "Top 'idle in transaction' states"
    }
  ]
}
```

* `id`: *(Optional)* Contains key from [`datasets`](#datasets) objects. Identifies values in dataset to build certain
table column be placed in table data cells `<td>`
* `class`: *(Optional)* Defines CSS class for this header tag `<th>`
* `caption`: Defines innerHTML for this header tag `<th>`
* `title`: *(Optional)* Defines title (html attribute) in table header tag `<td>`
* `columns`: *(Optional)* Nested `columns` attribute may exist if this column (tree-like)
divided into leaf columns. In this case `id` attribute will be absent, because data from
`datasets` can be define only for leaf columns.
* *Attention!* `id` and `title` could bear not just string, but two-element-length array
of string, e.g.:

```json
{
  "id": [
    "mean_exec_time1",
    "mean_exec_time2"
  ]
}
```
or
```json
{
  "title": [
    "properties.timePeriod1",
    "properties.timePeriod2"
  ]
}
```
It means that this table built by pairs of row (not by single rows), and each row in pair
has it's own `id` and `title`. Currently used for differential reports.

### <a id='filter'>Filter</a>
Section dataset can be filtered while building a report table. This attribute bears next keys:
* `type`: which is type or rule of dataset filtration. For instance:
  * `type: exists` means that filtered data should have any value (not null) in dataset attribute defined in `field`.
  * `type: equal` means that filtered data will have `value` in certain `field`

```json
{
  "header": [
    {
      "type": "row_table",
      "limit": "topn",
      "filter": {
        "type": "exists",
        "field": "ord_shared_blocks_dirt"
      },
      ...
    },
    ...
  ]
}
```
or
```json
{
  "header": [
    {
      "type": "row_table",
      "limit": "topn",
      "filter": {
        "type": "equal",
        "field": "flt_state_code",
        "value": 1
      },
      ...
    },
    ...
  ]
}
```
* `field`: attribute in [datasets](#datasets)
* `value`: *(Optional)* any comparable value

### <a id='preview'>Preview</a>
Preview feature helps to quickly find and preview the query text associated with a given table row.
To find this related query you need to have its `id` and specify the name of the `dataset`

```json
{
  "header": [
    {
      "type": "row_table",
      "limit": "topn",
      "filter": {},
      "source": "top_statements",
      "columns": [],
      "preview": [
        {
          "id": "hexqueryid",
          "dataset": "queries"
        }
      ]
    }
  ]
}
```
* `id`: id of associated query text
* `dataset`: name of [dataset](#datasets)

### <a id='highlight'>Highlight</a>
The feature helps highlight related table rows throughout the report.
To describe the highlighting rule, its needed to list all the [`dataset`](#datasets) attributes
which values must match in table rows

```json
{
  "header": [
    {
      "type": "row_table",
      "limit": "topn",
      "filter": {...},
      "source": "top_statements",
      "columns": [...],
      "preview": [...],
      "highlight": [
        {
          "id": "hexqueryid",
          "index": 0
        },
        {
          "id": "queryid",
          "index": 1
        },
        {
          "id": "dbname",
          "index": 1
        },
        {
          "id": "username",
          "index": 1
        }
      ]
    }
  ]
}
```
* `id`: name of attribute in `dataset`
* `index`: significance of attribute for future feature with partial highlight
