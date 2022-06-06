# graphconnect-2022-demo

![image](https://user-images.githubusercontent.com/9891346/172210966-e9e44a9f-e6e4-49b2-915c-b43d52569cec.png)

## Prerequisites

You'll need grab a copy of the source dataset from [OGB](https://ogb.stanford.edu/kddcup2021/mag240m/). Once you pull down the data, you'll need to stage it in BigQuery.

Spin up a VertexAI workbench and stage the files. You might need to make sure the service account backing the GCE VM has access to BigQuery.

Install Neo4j Enterprise 4.4 on a GCE VM. Make sure to install [GDS 2.1]([https://github.com/neo4j/graph](https://github.com/neo4j/graph-data-science/releases)) as well. In the `neo4j.conf` file, enable the Apache Arrow features by adding:

```
gds.arrow.listen_address=0.0.0.0:8491
gds.arrow.enabled=true
```
Lastly, make sure to setup the `neo4j` password. Put a copy of it in a new file called `pass.txt` in the notebook environment.

## Using the Notebooks

Run the [neo4j_arrow_mag240](./neo4j_arrow_mag240.ipynb) notebook to load the data.

The [GDS_code](./GDS_code.ipynb) contains examples using the [Neo4j GDS Client](https://github.com/neo4j/graph-data-science-client)
