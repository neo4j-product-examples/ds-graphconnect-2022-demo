# Tips for preparing the OGB Dataset

A bit about preparing the data in BigQuery...

## Initial Data Culling

The highlevel approach we took for the demo was to use Python to access the OGB
dataset, use Pandas to select out core data types from the dataset, and stage
them as Apache Parquet files in Google Cloud Storage.

```
$ ls -alh
total 11G
drwxrwxr-x 2 dave adm  4.0K May 17 23:23 .
drwxrwxr-x 4 dave dave 4.0K May 17 22:45 ..
-rw-rw-r-- 1 dave dave 7.8G May 17 22:54 citations.parquet
-rw-rw-r-- 1 dave dave 233M May 17 23:00 membership.parquet
-rw-rw-r-- 1 dave dave 571M May 17 23:23 papers.parquet
-rw-rw-r-- 1 dave dave 2.4G May 17 22:58 writes.parquet
```

* `citations.parquet` -- represents edges between `:Paper`s
* `membership.parquet` -- edges between `:Author`s and `:Institution`s
* `papers.parquet` -- all `:Paper` nodes with their properties.
* `writes.parquet` -- edges between `:Author`s and their `:Paper`s

The above parquet files were then staged in Google Cloud Storage to ease import
to a BigQuery dataset.


## Transforms in BigQuery

Once you have the data in BigQuery, we leveraged SQL to create tables for each
of our node labels and relationship types. We ended up with the following
tables in our dataset:

* `affiliation` -- `(author [int], institution [int], type [str])`
* `authors` -- `(author [int], label [str])`
* `authorsihp` -- `(author [int], paper [int], type [str])`
* `citations` -- `(source [int], target [int], type [str])`
* `institution` -- `(institution [int], labels [str])`
* `papers` -- `(paper [int], labels [str], years [int], flag [int])`

### Node Id Space
The current version of GDS (2.1) requires unique node ids so when we created
the above tables we shifted the original ids using the following approach of
adding a fixed offset:

* `author` -- original id + `0` (i.e. no shift)
* `institution` -- original id + `0xf800000`
* `paper` -- original id + `0x8000000`

