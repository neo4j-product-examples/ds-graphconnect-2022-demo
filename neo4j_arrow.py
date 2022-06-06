from typing import Any, Dict, Iterable, Union, Tuple
from enum import Enum
import json

import pyarrow as pa
import pyarrow.flight as flight


class ClientState(Enum):
    READY = "ready"
    FEEDING_NODES = "feeding_nodes"
    FEEDING_EDGES = "feeding_edges"
    AWAITING_GRAPH = "awaiting_graph"
    GRAPH_READY = "done"


class Neo4jArrowClient():
    def __init__(self, host: str, *, port: int=8491, user: str = "neo4j",
                 password: str = "neo4j", graph: str = "gcdemo", tls: bool = True,
                 concurrency: int = 4, database: str = "neo4j"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls = tls
        self.client: flight.FlightClient = None
        self.call_opts = None
        self.graph = graph
        self.database = database
        self.concurrency = concurrency
        self.state = ClientState.READY

    def __str__(self):
        return f"Neo4jArrowClient{{{self.user}@{self.host}:{self.port}/{self.graph}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "client" in state:
            del state["client"]
        if "call_opts" in state:
            del state["call_opts"]
        return state

    def copy(self):
        client = Neo4jArrowClient(self.host, port=self.port, user=self.user,
                                  password=self.password, graph=self.graph,
                                  tls=self.tls, concurrency=self.concurrency,
                                  database=self.database)
        client.state = self.state
        return client

    def _client(self):
        """Lazy client construction to help pickle this class."""
        if not hasattr(self, "client") or not self.client:
            self.call_opts = None
            if self.tls:
                location = flight.Location.for_grpc_tls(self.host, self.port)
            else:
                location = flight.Location.for_grpc_tcp(self.host, self.port)
            client = flight.FlightClient(location)
            if self.user and self.password:
                (header, token) = client.authenticate_basic_token(self.user, self.password)
                if header:
                    self.call_opts = flight.FlightCallOptions(headers=[(header, token)])
            self.client = client
        return self.client
        
    def _send_action(self, action: str, body: Dict[str, Any]) -> dict:
        """
        Communicates an Arrow Action message to the GDS Arrow Service.
        """
        client = self._client()
        try:
            payload = json.dumps(body).encode("utf-8")
            result = client.do_action(
                flight.Action(action, payload),
                options=self.call_opts
            )
            return json.loads(next(result).body.to_pybytes().decode())
        except Exception as e:
            print(f"send_action error: {e}")
            #return None
            raise e


    def _write_table(self, desc: bytes, table: pa.Table) -> Tuple[int, int]:
        """
        Write a PyArrow Table to the GDS Flight service.
        """
        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        writer, _ = client.do_put(upload_descriptor, table.schema, options=self.call_opts)
        with writer:
            try:
                writer.write_table(table)
                return table.num_rows, table.get_total_buffer_size()
            except Exception as e:
                print(f"_write_table error: {e}")
        return 0, 0

    @classmethod
    def _nop(*args, **kwargs):
        pass

    def _write_batches(self, desc: bytes, batches, mappingfn = None) -> Tuple[int, int]:
        """
        Write PyArrow RecordBatches to the GDS Flight service.
        """
        batches = iter(batches)
        fn = mappingfn or self._nop

        first = fn(next(batches, None))
        if not first:
            raise Exception("empty iterable of record batches provided")
        
        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        rows, nbytes = 0, 0
        writer, _ = client.do_put(upload_descriptor, first.schema, options=self.call_opts)
        with writer:
            try:
                writer.write_batch(first)
                rows += first.num_rows
                nbytes += first.get_total_buffer_size()
                for remaining in batches:
                    writer.write_batch(fn(remaining))
                    rows += remaining.num_rows
                    nbytes += remaining.get_total_buffer_size()
            except Exception as e:
                print(f"_write_batches error: {e}")
        return rows, nbytes

    def start(self, action: str = "CREATE_GRAPH", config: Dict[str, Any] = {}) -> Dict[str, Any]:
        assert self.state == ClientState.READY
        if not config:
            config = {
                "name": self.graph, 
                "database_name": self.database, 
                "concurrency": self.concurrency,
            }
        result = self._send_action(action, config)
        if result:
            self.state = ClientState.FEEDING_NODES
        return result

    def write_nodes(self, nodes: Union[pa.Table, Iterable[pa.RecordBatch]], mappingfn = None) -> Tuple[int, int]:
        assert self.state == ClientState.FEEDING_NODES
        desc = { "name": self.graph, "entity_type": "node" }
        if isinstance(nodes, pa.Table):
            return self._write_table(desc, nodes, mappingfn)
        return self._write_batches(desc, nodes, mappingfn)

    def nodes_done(self) -> Dict[str, Any]:
        assert self.state == ClientState.FEEDING_NODES
        result = self._send_action("NODE_LOAD_DONE", { "name": self.graph })
        if result:
            self.state = ClientState.FEEDING_EDGES
        return result

    def write_edges(self, edges: Union[pa.Table, Iterable[pa.RecordBatch]], mappingfn = None) -> Tuple[int, int]:
        assert self.state == ClientState.FEEDING_EDGES
        desc = { "name": self.graph, "entity_type": "relationship" }
        if isinstance(edges, pa.Table):
            return self._write_table(desc, edges, mappingfn)
        return self._write_batches(desc, edges, mappingfn)

    def edges_done(self) -> Dict[str, Any]:
        assert self.state == ClientState.FEEDING_EDGES
        result = self._send_action("RELATIONSHIP_LOAD_DONE",
                                   { "name": self.graph })
        if result:
            self.state = ClientState.AWAITING_GRAPH
        return result

    def read_edges(self, prop: str):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamRelationshipProperty",
            "configuration": {
                "relationship_types": "*", 
                "relationship_property": prop,
            },
            "concurrency": 224,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk

    def read_nodes(self, prop: str):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamNodeProperty",
            "configuration": { 
                "node_labels": "*", 
                "node_property": prop,
            },
            "concurrency": 224,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk


    def wait(timeout: int = 0):
        """wait for completion"""
        assert self.state == ClientState.AWAITING_GRAPH
        self.state = ClientState.AWAITING_GRAPH
        # TODO: return future? what do we do?
        pass
