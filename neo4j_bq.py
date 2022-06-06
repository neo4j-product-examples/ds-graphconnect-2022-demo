from typing import Any, Callable, Dict, List, Union, Tuple

import base64, pickle, os, sys, time
import multiprocessing as mp
import subprocess as sub

from google.cloud.bigquery_storage import BigQueryReadClient, types

import pyarrow as pa
import neo4j_arrow as na


class BigQuerySource:
    """
    Wrapper around a BigQuery Dataset. Uses the Storage API to generate a list
    of streams that the BigQueryReadClient can fetch.
    """
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        
    def __str__(self):
        return f"BigQuerySource{{project_id={project_id}, dataset={dataset}}}"

    def table(self, table:str, *, fields: List[str] = [], 
              max_stream_count:int = 8_192 * 2) -> list:
        """Get one or many Arrow-based streams for a given BigQuery table."""
        _client = BigQueryReadClient()
        read_session = types.ReadSession(
            table=f"projects/{self.project_id}/datasets/{self.dataset}/tables/{table}",
            data_format=types.DataFormat.ARROW
        )
        if fields:
            read_session.read_options.selected_fields=fields

        session = _client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=read_session,
            max_stream_count=max_stream_count,
        )
        return [stream.name for stream in session.streams]


def streams_from(node_or_edge: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform a node/edge dict into a list of single-stream work units."""
    streams = []
    for stream in node_or_edge.get("streams"):
        d = dict(
            [("stream", stream)] + 
            [(k, v) for k, v in node_or_edge.items() if not k == "streams"]
        )
        streams.append(d)
    return streams


_worker_na_client = None
def _initializer(client: na.Neo4jArrowClient):
    """Initializer for our multiprocessing Pool members."""
    global _worker_na_client
    _worker_na_client = client


def _process_nodes(nodes, **kwargs) -> Tuple[int, int]:
    """Streams the given PyArrow table to the Neo4j server using a Neo4jArrowClient."""
    global _worker_na_client
    assert _worker_na_client
    
    # Perform last mile renaming of any fields in our PyArrow Table
    key, labels = kwargs.get("key"), kwargs.get("labels")
    def map_batch(batch):
        new_schema = batch.schema
        for idx, name in enumerate(batch.schema.names): # assumption: they're in the column order
            field = new_schema.field(name)
            if name == key:
                new_schema = new_schema.set(idx, field.with_name("nodeId"))
            elif name == labels:
                new_schema = new_schema.set(idx, field.with_name("labels"))
        return batch.from_arrays(batch.columns, schema=new_schema)
    
    # feed the graph
    return _worker_na_client.write_nodes(nodes, map_batch)


def _process_edges(edges, **kwargs) -> Tuple[int, int]:
    """Streams the given PyArrow table to the Neo4j server using a Neo4jArrowClient."""
    global _worker_na_client
    assert _worker_na_client
    
    # Perform last mile renaming of any fields in our PyArrow Table/Recordbatch
    src, dst, _type = kwargs.get("src"), kwargs.get("dst"), kwargs.get("type")
    def map_batch(batch):
        new_schema = batch.schema
        for idx, name in enumerate(batch.schema.names): # assumption: they're in the column order
            field = new_schema.field(name)
            if name == src:
                new_schema = new_schema.set(idx, field.with_name("sourceNodeId"))
            elif name == dst:
                new_schema = new_schema.set(idx, field.with_name("targetNodeId"))
            elif name == _type:
                new_schema = new_schema.set(idx, field.with_name("relationshipType"))
        return batch.from_arrays(batch.columns, schema=new_schema)
    
    # feed the graph
    return _worker_na_client.write_edges(edges, map_batch)


def worker(work: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Main logic for our subprocessing children"""
    client = BigQueryReadClient()
    
    name = f"worker-{os.getpid()}"
    if isinstance(work, dict):
        work = [work]
    
    def consume_stream(consumer, **kwargs):
        """Apply consumer to a stream in the form of a generator"""
        stream = kwargs["stream"]
        reader = client.read_rows(stream)
        rows = reader.rows()
        
        def batch_generator():
            for page in rows.pages:
                yield page.to_arrow()
        yield consumer(batch_generator(), **kwargs)

    total_rows, total_bytes = 0, 0    
    
    # For now, we identify the work type based on its schema
    for task in work:
        if "key" in task:
            fn = _process_nodes
        elif "src" in task:
            fn = _process_edges
        else:
            raise Exception(f"{name} can't pick a consuming function")
        for rows, nbytes in consume_stream(fn, **task):
            total_rows += rows
            total_bytes += nbytes
    return {"name": name, "rows": total_rows, "bytes": total_bytes}


def flatten(lists: List[List[Any]], fn: Callable) -> List[Any]:
    """Helper function...collapse list of lists into a single list."""
    return [x for y in map(fn, lists) for x in y]



###############################################################################
###############################################################################
#    _   _            _  _   _              _                           
#   | \ | | ___  ___ | || | (_)    _       / \   _ __ _ __ _____      __
#   |  \| |/ _ \/ _ \| || |_| |  _| |_    / _ \ | '__| '__/ _ \ \ /\ / /
#   | |\  |  __/ (_) |__   _| | |_   _|  / ___ \| |  | | | (_) \ V  V / 
#   |_| \_|\___|\___/   |_|_/ |   |_|   /_/   \_\_|  |_|  \___/ \_/\_/  
#                         |__/                                          
#              __  __             _                                     
#      _____  |  \/  | __ _  __ _(_) ___                                
#     |_____| | |\/| |/ _` |/ _` | |/ __|                               
#     |_____| | |  | | (_| | (_| | | (__                                
#             |_|  |_|\__,_|\__, |_|\___|                               
#                           |___/   
###############################################################################
#
#  Below this point is the main entrypoint for the worker processes. Do not
#  change this area if you don't know what you're doing ;-)
#
###############################################################################

def fan_out(client: na.Neo4jArrowClient, work: List[str],
            processes: int = 0, timeout: int = 120) -> Tuple[List[Any], float]:
    """
    This is where the magic happens. Pop open a subprocess that execs this same
    module. Once the child is alive, send it some pickled objects to bootstrap
    the workload. The child will drive the worker pool and communicate back
    data via stdout and messaging via stderr.
    
    This design solves problems with Jupyter kernels mismanaging children.
    """
    config = { "processes": processes, "client": client.copy() }
    #payload = base64.b64encode(pickle.dumps((config, work)))
    payload = pickle.dumps((config, work))

    argv = [sys.executable, "./neo4j_bq.py"]
    with sub.Popen(argv, stdin=sub.PIPE, stdout=sub.PIPE) as proc:
        try:
            (out, _) = proc.communicate(payload, timeout=timeout)
            #(res, delta) = pickle.loads(base64.b64decode(out))
            (res, delta) = pickle.loads(out)
            return (res, delta)
        except sub.TimeoutExpired as to_err:
            print(f"timed out waiting for subprocess response...killing child")
            proc.terminate()
            return ([], 0)
        

if __name__ == "__main__":
    results, delta = [], 0.0
    
    def log(msg, newline=True):
        """Write to stderr to send messages 'out of band' and back to Jupyter."""
        if newline:
            sys.stderr.write(f"{msg}{os.linesep}")
        else:
            sys.stderr.write(f"{msg}")
            sys.stderr.flush()
    
    try:
        # Read our payload from stdin and unpickle
        (config, work) = pickle.load(sys.stdin.buffer)
        
        client = config["client"]
        log(f"Using: 🚀 {client}")

        processes = min(len(work), config.get("processes") or int(mp.cpu_count() * 1.3))
        log(f"Spawning {processes:,} workers 🧑‍🏭 to process {len(work):,} tasks 📋")
        
        # Make a pretty progress bar
        ticks = [n for n in range(1, len(work), int(len(work) / 33))] + [len(work)]
        ticks.reverse()
        
        mp.set_start_method("fork")
        with mp.Pool(processes=processes, initializer=_initializer,
                    initargs=[client]) as pool:
            
            # The main processing loop
            log("⚙️ Loading: [", newline=False)
            start = time.time()
            for result in pool.imap_unordered(worker, work):
                results.append(result)
                if ticks and len(results) == ticks[-1]:
                    log("➶", newline=False)
                    ticks.pop()
            log("]\n", newline=False) 
            delta = time.time() - start
        log(f"🏁 Completed in {round(delta, 2)}s")
    except Exception as e:
        log(f"⚠️ Error: {e}")
    
    pickle.dump((results, delta), sys.stdout.buffer)
