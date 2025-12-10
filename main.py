import json

import eel
import time
from mongo_db_manager import MongoDBManager
from neo4j_manager import Neo4jManager
import datetime

# Initialize Eel with the web folder
eel.init('web')

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "social_network_document_database"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "p4ssw0rd"

# Query 1: Location Finder
@eel.expose
def execute_query_1(person_id):
    """
    Identify the location of the university where a certain person studied and the location of the company where they work.
    """
    manager = MongoDBManager(MONGO_URI, MONGO_DB)
    try:
        result = manager.get_person_locations(person_id)
        if "error" in result:
            raise Exception(result["error"])
        else:
            result = {
                "state": "success",
                "result": result
            }
    except Exception as e:
        print(f"Error executing query: {e}")
        result = {
            "state": "error",
            "result": f"Error executing query: {e}"
        }

    finally:
        manager.close()

    return result


# Query 2: Known Colleagues
@eel.expose
def execute_query_2(param1):
    """
    Given a person, identify all other people they know within the company where they work or the university where they study.
    """
    mongo_manager = MongoDBManager(MONGO_URI, MONGO_DB)
    neo4j_manager = Neo4jManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        person_id = str(param1)
        university_colleagues = mongo_manager.get_university_colleagues(person_id)
        work_colleagues = mongo_manager.get_work_colleagues(person_id)

        # errors in colleagues retrive (es. the person does not exists)
        if "error" in university_colleagues:
            raise Exception(university_colleagues["error"])

        if "error" in work_colleagues:
            raise Exception(work_colleagues["error"])

        # If the person did not attend university
        if not university_colleagues:
            university = {"total_colleagues": 0, "university_colleagues": "The person did not attend university."}
        else:
            university_known = neo4j_manager.get_known_from_list(person_id, university_colleagues)
            if not university_known:
                university_known = "The person not known any colleague in the university."
            university = {
                "Total Colleagues": len(university_colleagues),
                "Total Known Colleagues": len(university_known),
                "Known Colleagues": university_known
            }

        # if the person doen't work
        if not work_colleagues:
            work = {"Total Colleagues": 0, "Known Colleagues": "The person does not work."}
        else:
            work_known = neo4j_manager.get_known_from_list(person_id, work_colleagues)
            if not work_known:
                work_known = "The person not known any colleague in the company."
            work = {
                "Total Colleagues": len(work_colleagues),
                "Total Known Colleagues": len(work_known),
                "Known Colleagues": work_known
            }

        result = {
            "state": "success",
            "result": {
                    "University": university,
                    "Company": work
                }
        }
    except Exception as e:
        result = {
            "state": "error",
            "result": f"Error executing query: {e}"
        }
    finally:
        mongo_manager.close()
        neo4j_manager.close()

    return result


# Query 3: Most Likes
@eel.expose
def execute_query_3():
    """
    Find the most popular person in terms of total likes across all their posts.
    """

    manager = Neo4jManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        top_creator = manager.get_most_liked_person()
        result = {
            "state": "success",
            "result": top_creator
        }

    except Exception as e:
        print(f"Error executing query: {e}")
        result = {
            "state": "error",
            "result": f"Error executing query: {e}"
        }

    finally:
        manager.close()

    return result


# Query 4: Top Tag
@eel.expose
def execute_query_4(param1, param2):
    """
    Find the tag with the most usage during a given time period.
    """

    manager = Neo4jManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        begin_date_elements = param1.split('-')
        end_date_elements = param2.split('-')

        begin_date = datetime.datetime(int(begin_date_elements[0]), # year
                                       int(begin_date_elements[1]), # month
                                       int(begin_date_elements[2]), # day
                                       tzinfo=datetime.timezone.utc)

        end_date = datetime.datetime(int(end_date_elements[0]),
                                     int(end_date_elements[1]),
                                     int(end_date_elements[2]),
                                     tzinfo=datetime.timezone.utc)

        top_tag = manager.get_most_used_tag(begin_date, end_date)
        print(top_tag)
        result = {
            "state": "success",
            "result": top_tag
        }

    except Exception as e:
        print(f"Error executing query: {e}")
        result = {
            "state": "error",
            "result": f"Error executing query: {e}"
        }

    finally:
        manager.close()

    return result

# Query 5: Most Influent Person
@eel.expose
def execute_query_5(param1):
    """
    Identify the most popular user within a university (in terms of people who know them)
    """
    mongo_manager = MongoDBManager(MONGO_URI, MONGO_DB)
    neo4j_manager = Neo4jManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        university_id = str(param1)
        university_students = mongo_manager.get_university_students(university_id)

        # errors in colleagues retrive (es. the person does not exists)
        if "error" in university_students:
            raise Exception(university_students["error"])


        # If the university has no students
        if not university_students:
            result = {
                "state": "success",
                "result": {
                    "message": "The university has no students registered in the database."
                }
            }

        most_known = neo4j_manager.get_most_popular_in_list(university_students)
        most_known_person_id = most_known["KnownPersonId"]
        known_count = most_known["KnownCount"]

        most_known_person = mongo_manager.get_person_info(most_known_person_id)


        result = {
            "state": "success",
            "result": {
                "Person": most_known_person,
                "KnownCount": known_count,
                "TotalStudents": len(university_students)
            }
        }

    except Exception as e:
        result = {
            "state": "error",
            "result": f"Error executing query: {e}"
        }
    finally:
        mongo_manager.close()
        neo4j_manager.close()

    return result



# start the application
if __name__ == '__main__':
    eel.start('index.html', size=(1400, 900))