from typing import List, Union, Any

from neo4j import GraphDatabase, Neo4jDriver, Session
from neo4j.exceptions import Neo4jError
from kgx.config import get_logger
from kgx.error_detection import ErrorType
from kgx.sink.sink import Sink
from kgx.source.source import DEFAULT_NODE_CATEGORY
log = get_logger()


class NeoSink(Sink):
    """
    NeoSink is responsible for writing data as records
    to a Neo4j instance.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs
    uri: str
        The URI for the Neo4j instance.
        For example, http://localhost:7474
    username: str
        The username
    password: str
        The password
    kwargs: Any
        Any additional arguments

    """

    CACHE_SIZE = 100000
    node_cache = {}
    edge_cache = {}
    node_count = 0
    edge_count = 0
    CATEGORY_DELIMITER = "|"
    CYPHER_CATEGORY_DELIMITER = ":"
    _seen_categories = set()

    def __init__(self, owner, uri: str, username: str, password: str, **kwargs: Any):
        log.trace("NeoSink.__init__ called with uri=%s", uri)
        if "cache_size" in kwargs:
            self.CACHE_SIZE = kwargs["cache_size"]
        log.info("Connecting to Neo4j at %s", uri)
        log.debug("Neo4j sink cache size set to %s", self.CACHE_SIZE)
        try:
            self.http_driver:Neo4jDriver = GraphDatabase.driver(
                uri, auth=(username, password)
            )
        except Neo4jError as e:
            log.error(
                "Neo4j refused connection for %s: %s", uri, e, exc_info=True
            )
            raise
        except Exception as e:
            log.error(
                "Unexpected error establishing Neo4j driver for %s: %s",
                uri,
                e,
                exc_info=True,
            )
            raise
        try:
            self.session: Session = self.http_driver.session()
        except Neo4jError as e:
            log.error(
                "Neo4j session creation failed for %s: %s", uri, e, exc_info=True
            )
            raise
        except Exception as e:
            log.error(
                "Unexpected error opening Neo4j session for %s: %s",
                uri,
                e,
                exc_info=True,
            )
            raise
        log.trace("NeoSink session created: %s", self.session)
        super().__init__(owner)

    def _flush_node_cache(self):
        log.trace("NeoSink._flush_node_cache invoked")
        total_nodes = sum(len(nodes) for nodes in self.node_cache.values())
        if total_nodes:
            log.info(
                "Flushing %s cached nodes to Neo4j", total_nodes
            )
        self._write_node_cache()
        self.node_cache.clear()
        self.node_count = 0

    def write_node(self, record) -> None:
        """
        Cache a node record that is to be written to Neo4j.
        This method writes a cache of node records when the
        total number of records exceeds ``CACHE_SIZE``

        Parameters
        ----------
        record: Dict
            A node record

        """
        log.trace("NeoSink.write_node invoked for node %s", record.get("id"))
        sanitized_category = self.sanitize_category(record["category"])
        category = self.CATEGORY_DELIMITER.join(sanitized_category)
        if self.node_count >= self.CACHE_SIZE:
            self._flush_node_cache()
        if category not in self.node_cache:
            self.node_cache[category] = [record]
        else:
            self.node_cache[category].append(record)
        self.node_count += 1

    def _write_node_cache(self) -> None:
        """
        Write cached node records to Neo4j.
        """
        log.trace("NeoSink._write_node_cache invoked")
        batch_size = 10000
        categories = self.node_cache.keys()
        filtered_categories = [x for x in categories if x not in self._seen_categories]
        self.create_constraints(filtered_categories)
        total_nodes = sum(len(nodes) for nodes in self.node_cache.values())
        if total_nodes:
            log.info(
                "Uploading %s nodes across %s categories", total_nodes, len(self.node_cache)
            )
        for category in self.node_cache.keys():
            log.debug("Generating UNWIND for category: {}".format(category))
            cypher_category = category.replace(
                self.CATEGORY_DELIMITER, self.CYPHER_CATEGORY_DELIMITER
            )
            query = self.generate_unwind_node_query(cypher_category)

            log.debug(query)
            nodes = self.node_cache[category]
            for x in range(0, len(nodes), batch_size):
                y = min(x + batch_size, len(nodes))
                log.debug(f"Batch {x} - {y}")
                batch = nodes[x:y]
                try:
                    result = self.session.run(query, parameters={"nodes": batch})
                    summary = result.consume()
                    log.debug(
                        "Neo4j summary for nodes in category %s (batch %s-%s): %s",
                        category,
                        x,
                        y,
                        summary.counters,
                    )
                except Neo4jError as e:
                    log.error(
                        "Neo4j error uploading nodes for category %s batch %s-%s: %s",
                        category,
                        x,
                        y,
                        e,
                        exc_info=True,
                    )
                    self.owner.log_error(
                        entity=f"{category} Nodes {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )
                except Exception as e:
                    log.error(
                        "Error uploading nodes for category %s batch %s-%s: %s",
                        category,
                        x,
                        y,
                        e,
                    )
                    self.owner.log_error(
                        entity=f"{category} Nodes {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    def _flush_edge_cache(self):
        log.trace("NeoSink._flush_edge_cache invoked")
        self._flush_node_cache()
        self._write_edge_cache()
        self.edge_cache.clear()
        self.edge_count = 0

    def write_edge(self, record) -> None:
        """
        Cache an edge record that is to be written to Neo4j.
        This method writes a cache of edge records when the
        total number of records exceeds ``CACHE_SIZE``

        Parameters
        ----------
        record: Dict
            An edge record

        """
        log.trace(
            "NeoSink.write_edge invoked for edge %s", record.get("id")
        )
        if self.edge_count >= self.CACHE_SIZE:
            self._flush_edge_cache()
        # self.validate_edge(data)
        edge_predicate = record["predicate"]
        if edge_predicate in self.edge_cache:
            self.edge_cache[edge_predicate].append(record)
        else:
            self.edge_cache[edge_predicate] = [record]
        self.edge_count += 1

    def _write_edge_cache(self) -> None:
        """
        Write cached edge records to Neo4j.
        """
        log.trace("NeoSink._write_edge_cache invoked")
        batch_size = 10000
        total_edges = sum(len(edges) for edges in self.edge_cache.values())
        if total_edges:
            log.info(
                "Uploading %s edges across %s predicates", total_edges, len(self.edge_cache)
            )
        for predicate in self.edge_cache.keys():
            query = self.generate_unwind_edge_query(predicate)
            log.debug(query)
            edges = self.edge_cache[predicate]
            for x in range(0, len(edges), batch_size):
                y = min(x + batch_size, len(edges))
                batch = edges[x:y]
                log.debug(f"Batch {x} - {y}")
                log.debug(edges[x:y])
                try:
                    result = self.session.run(
                        query, parameters={"relationship": predicate, "edges": batch}
                    )
                    summary = result.consume()
                    log.debug(
                        "Neo4j summary for predicate %s (batch %s-%s): %s",
                        predicate,
                        x,
                        y,
                        summary.counters,
                    )
                except Neo4jError as e:
                    log.error(
                        "Neo4j error uploading edges for predicate %s batch %s-%s: %s",
                        predicate,
                        x,
                        y,
                        e,
                        exc_info=True,
                    )
                    self.owner.log_error(
                        entity=f"{predicate} Edges {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )
                except Exception as e:
                    log.error(
                        "Error uploading edges for predicate %s batch %s-%s: %s",
                        predicate,
                        x,
                        y,
                        e,
                    )
                    self.owner.log_error(
                        entity=f"{predicate} Edges {batch}",
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    def finalize(self) -> None:
        """
        Write any remaining cached node and/or edge records.
        """
        log.trace("NeoSink.finalize invoked")
        log.info("Finalizing Neo4j upload")
        self._write_node_cache()
        self._write_edge_cache()

    @staticmethod
    def sanitize_category(category: List) -> List:
        """
        Sanitize category for use in UNWIND cypher clause.
        This method adds escape characters to each element in category
        list to ensure the category is processed correctly.

        Parameters
        ----------
        category: List
            Category

        Returns
        -------
        List
            Sanitized category list

        """
        log.trace("NeoSink.sanitize_category invoked for %s", category)
        return [f"`{x}`" for x in category]

    @staticmethod
    def generate_unwind_node_query(category: str) -> str:
        """
        Generate UNWIND cypher query for saving nodes into Neo4j.

        There should be a CONSTRAINT in Neo4j for ``self.DEFAULT_NODE_CATEGORY``.
        The query uses ``self.DEFAULT_NODE_CATEGORY`` as the node label to increase speed for adding nodes.
        The query also sets label to ``self.DEFAULT_NODE_CATEGORY`` for any node to make sure that the CONSTRAINT applies.

        Parameters
        ----------
        category: str
            Node category

        Returns
        -------
        str
            The UNWIND cypher query

        """
        log.trace("NeoSink.generate_unwind_node_query invoked for %s", category)
        query = f"""
        UNWIND $nodes AS node
        MERGE (n:`{DEFAULT_NODE_CATEGORY}` {{id: node.id}})
        ON CREATE SET n += node, n:{category}
        ON MATCH SET n += node, n:{category}
        """

        return query

    @staticmethod
    def generate_unwind_edge_query(edge_predicate: str) -> str:
        """
        Generate UNWIND cypher query for saving edges into Neo4j.

        Query uses ``self.DEFAULT_NODE_CATEGORY`` to quickly lookup the required subject and object node.

        Parameters
        ----------
        edge_predicate: str
            Edge label as string

        Returns
        -------
        str
            The UNWIND cypher query

        """
        log.trace(
            "NeoSink.generate_unwind_edge_query invoked for %s", edge_predicate
        )

        query = f"""
        UNWIND $edges AS edge
        MATCH (s:`{DEFAULT_NODE_CATEGORY}` {{id: edge.subject}}), (o:`{DEFAULT_NODE_CATEGORY}` {{id: edge.object}})
        MERGE (s)-[r:`{edge_predicate}`]->(o)
        SET r += edge
        """
        return query

    def create_constraints(self, categories: Union[set, list]) -> None:
        """
        Create a unique constraint on node 'id' for all ``categories`` in Neo4j.

        Parameters
        ----------
        categories: Union[set, list]
            Set of categories

        """
        log.trace(
            "NeoSink.create_constraints invoked for categories: %s", categories
        )
        categories_set = set(categories)
        categories_set.add(f"`{DEFAULT_NODE_CATEGORY}`")
        if categories_set:
            log.info(
                "Ensuring Neo4j constraints exist for %s categories", len(categories_set)
            )
        for category in categories_set:
            if self.CATEGORY_DELIMITER in category:
                subcategories = category.split(self.CATEGORY_DELIMITER)
                self.create_constraints(subcategories)
            else:
                query = NeoSink.create_constraint_query(category)
                try:
                    result = self.session.run(query)
                    summary = result.consume()
                    log.debug(
                        "Neo4j constraint summary for category %s: %s",
                        category,
                        summary.counters,
                    )
                    self._seen_categories.add(category)
                except Neo4jError as e:
                    log.error(
                        "Neo4j error creating constraint for category %s: %s",
                        category,
                        e,
                        exc_info=True,
                    )
                    self.owner.log_error(
                        entity=category,
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )
                except Exception as e:
                    log.error(
                        "Error creating constraint for category %s: %s",
                        category,
                        e,
                    )
                    self.owner.log_error(
                        entity=category,
                        error_type=ErrorType.INVALID_CATEGORY,
                        message=str(e)
                    )

    @staticmethod
    def create_constraint_query(category: str) -> str:
        """
        Create a Cypher CONSTRAINT query

        Parameters
        ----------
        category: str
            The category to create a constraint on

        Returns
        -------
        str
            The Cypher CONSTRAINT query

        """
        log.trace(
            "NeoSink.create_constraint_query invoked for category: %s", category
        )
        query = f"CREATE CONSTRAINT IF NOT EXISTS ON (n:{category}) ASSERT n.id IS UNIQUE"
        return query
