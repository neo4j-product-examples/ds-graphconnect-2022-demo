# graphconnect-2022-demo

![image](https://user-images.githubusercontent.com/9891346/172210966-e9e44a9f-e6e4-49b2-915c-b43d52569cec.png)

## Prerequisites

You'll need grab a copy of the source dataset from
[OGB](https://ogb.stanford.edu/kddcup2021/mag240m/). Once you pull down the
data, you'll need to stage it in BigQuery.

Spin up a VertexAI workbench and stage the files. You might need to make sure
the service account backing the GCE VM has access to BigQuery.

### Installing Neo4j
Install `Neo4j Enterprise v4.4` on a GCE VM. (You can install it anywhere you
like, but we'll be using a GCP environment to make integration with
BigQuery easier.)

#### Install Bloom

Follow the [Bloom docs](https://neo4j.com/docs/bloom-user-guide/current/bloom-installation/)
and install Bloom.

Once you have Bloom running, you can import the [perspective](./PaperPerspective.json).

#### Install GDS

Make sure to install [GDS 2.1](https://github.com/neo4j/graph-data-science/releases)
as well. In the `neo4j.conf` file, enable the Apache Arrow features by adding:

```properties
gds.arrow.listen_address=0.0.0.0:8491
gds.arrow.enabled=true
```

#### Configuring Neo4j

If you're using the full dataset, we recommend about 512g of heap for the jvm
if possible.

```properties
dbms.memory.heap.initial_size=512g
dbms.memory.heap.max_size=512g
```

> Setting the `initial_size` will preallocate memory for the jvm during
> startup of Neo4j.

#### Configure the `neo4j` user

Lastly, make sure to setup the `neo4j` password. Put a copy of it in a new file
called `pass.txt` in the notebook environment.

## Using the Notebooks

Run the [neo4j_arrow_mag240](./neo4j_arrow_mag240.ipynb) notebook to load the
data.

The [GDS_code](./GDS_code.ipynb) notebook contains examples using the
[Neo4j GDS Client](https://github.com/neo4j/graph-data-science-client)
