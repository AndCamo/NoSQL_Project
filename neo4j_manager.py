from dns.e164 import query
from future.backports.datetime import tzinfo
from neo4j import GraphDatabase
import logging
import datetime


class Neo4jManager:
    def __init__(self, uri, user, password):
        """
        Initializes the Neo4j driver and start connection
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        Close the Neo4j driver connection
        """
        self.driver.close()

    def clear_database(self):
        """
        Clears the database by deleting all nodes and relationships.
        """
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.")

    def load_people(self, csv_file):
        """Loads Person nodes from LDBC csv file."""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        MERGE (p:Person {id: row.id})
        SET p.firstName = row.firstName,
            p.lastName = row.lastName
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"People Nodes loaded")

    def load_posts(self, csv_file):
        """Loads Post nodes and creates [:CREATED] relation to Person."""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        MERGE (post:Post {id: row.id})
        WITH post, row
        MATCH (creator:Person {id: row.CreatorPersonId})
        MERGE (creator)-[:CREATED {creationDate : datetime(row.creationDate)}]->(post)
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"Posts Nodes and CREATED relation loaded")

    def load_likes_edges(self, csv_file):
        """Loads 'LIKES' relationships between Person and Post nodes."""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        MATCH (per:Person {id: row.PersonId})
        MATCH (pos:Post {id: row.PostId})
        MERGE (per)-[:LIKES]->(pos)
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"LIKES edges loaded")

    def load_tags_edges(self, csv_file):
        """Loads Tag noted and create 'HASTAG' relationships between Post and Tag."""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        
        MERGE (tag:Tag {id: row.TagId})
        WITH tag, row
        MATCH (post:Post {id: row.PostId})
        MERGE (post)-[:HASTAG {creationDate : datetime(row.creationDate)}]->(tag)
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"HASTAG edges loaded")

    def load_tags_info(self, csv_file):
        """ Set the name to TAGS nodes"""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        MATCH (tag:Tag {id: row.id})
        SET tag.name = row.name
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"Tags Nodes loaded")

    def load_knows_edges(self, csv_file):
        """Loads 'KNOWS' relationships between people."""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row FIELDTERMINATOR '|'
        MATCH (p1:Person {id: row.Person1Id})
        MATCH (p2:Person {id: row.Person2Id})
        MERGE (p1)-[:KNOWS]->(p2)
        """
        with self.driver.session() as session:
            session.run(query, file=csv_file)
            print(f"KNOWS edges loaded")

    def get_most_liked_person(self):
        """Returns the person with the most likes (across all posts)."""
        query = """
        MATCH (author:Person)-[:CREATED]->(post:Post)<-[like:LIKES]-(:Person)
        RETURN author.firstName AS Name, 
               author.lastName AS Surname, 
               count(like) AS TotalLikes
        ORDER BY TotalLikes DESC
        LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()
            if record:
                return record.data()

            return None

    def get_most_used_tag(self, begin_date, end_date):
        """Returns the tag with the most usages (posts) during a given time period."""
        query = """
        MATCH (:Person)-[r:CREATED]->(post:Post)-[:HASTAG]->(tag:Tag)
        WHERE r.creationDate >= $begin_date AND r.creationDate <= $end_date
        RETURN tag.name AS TagName,
               count(post) AS TotalUsages
        ORDER BY TotalUsages DESC
        LIMIT 5
        """
        with self.driver.session() as session:
            result = session.run(query, begin_date=begin_date, end_date=end_date)
            return [record.data() for record in result]

    def get_known_from_list(self, person_id, id_list):
        """Returns the people that the given person knows from the given list."""
        query = """
            MATCH (person:Person {id: $person_id})-[:KNOWS]->(known:Person)
            WHERE known.id IN $id_list
            RETURN known.id AS KnownPersonId, known.firstName AS KnownFirstName, known.lastName AS KnownLastName
        """
        with self.driver.session() as session:
            result = session.run(query, person_id=person_id, id_list=id_list)
            return [f"{record.data()["KnownFirstName"]} {record.data()["KnownLastName"]} ({record.data()["KnownPersonId"]})" for record in result]

    def get_most_popular_in_list(self, person_ids):
        """ get the most known person from a list of people """
        query = """
            MATCH (person:Person)-[:KNOWS]->(known:Person)
            WHERE known.id IN $person_ids
            RETURN known.id AS KnownPersonId, count(person) AS KnownCount
            ORDER BY KnownCount DESC
            LIMIT 1
        """
        with self.driver.session() as session:
            result = session.run(query, person_ids=person_ids)
            record = result.single()
            if record:
                return record.data()

            return None

    def get_known_people(self, person_id):
        """Returns all the people that the given person knows."""
        query = """
            MATCH (person:Person)-[:KNOWS]->(known:Person)
            WHERE person.id = $person_id
            RETURN known.id AS KnownPersonId
        """
        with self.driver.session() as session:
            result = session.run(query, person_id=person_id)
            return [record.data()["KnownPersonId"] for record in result]


    def load_data(self):
        """wrapper for all the load functions to load the data in the database."""
        self.clear_database()

        self.load_people(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/dynamic/Person.csv")
        self.load_knows_edges(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/dynamic/Person_knows_Person.csv")
        self.load_posts(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/dynamic/Post.csv")
        self.load_likes_edges(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/dynamic/Person_likes_Post.csv")
        self.load_tags_edges(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/dynamic/Post_hasTag_Tag.csv")
        self.load_tags_info(
            "/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/static/Tag.csv")

        # Create Indexes
        with self.driver.session() as session:
            session.run("CREATE INDEX person_id IF NOT EXISTS FOR (p:Person) ON (p.id)")
            session.run("CREATE INDEX post_id IF NOT EXISTS FOR (p:Post) ON (p.id)")
            session.run("CREATE INDEX tag_id IF NOT EXISTS FOR (t:Tag) ON (t.id)")
            print("Index created.")



if __name__ == "__main__":
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "p4ssw0rd"

    db = Neo4jManager(URI, USER, PASSWORD)

    try:


        # ====== DATA LOADING ======
        # db.load_data()
        pass

    finally:
        db.close()