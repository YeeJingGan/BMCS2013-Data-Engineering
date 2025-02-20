"""
Author: Yeap Jie Shen
"""
from neo4j import GraphDatabase

class Neo4j:
    """
        This class is a client class for Neo4J Aura DB
    """
    def __init__(self, URI = 'neo4j+s://6ca73887.databases.neo4j.io', AUTH = ('neo4j', 'MTIIDXuTsh7beWIEagepsrs_h1fj2ioZjUGbkBN118o')):
        """
            DEFAULT URI -> neo4j+s://6ca73887.databases.neo4j.io
            DEFAULT AUTH -> ('neo4j', 'MTIIDXuTsh7beWIEagepsrs_h1fj2ioZjUGbkBN118o')

            URI is the Uniform Resource Identifier to the cluster
            AUTH is a tuple of (username, password)
        """
        self.URI = URI
        self.AUTH = AUTH

        self.test_connection()

    def test_connection(self):
        with GraphDatabase.driver(self.URI, auth = self.AUTH) as driver:
            try:
                driver.verify_connectivity()
                print('Connection established')
            except Exception as e:
                print('Some error occured')
                print(e)

    def execute_query(self, query, database_ = 'neo4j', **kwargs):
        """
            Execute a query written in Cypher, with the specified database
            
            **kwargs is for substituting parameters in the query with arguments

            Note: Parameters in Cypher are preceeded with a dollar sign '$' followed by the parameter name
        """
        with GraphDatabase.driver(self.URI, auth = self.AUTH) as driver:
            try:
                records, summary, keys = driver.execute_query(query, **kwargs)
            except Exception as e:
                print(e)
            finally:
                return records, summary, keys