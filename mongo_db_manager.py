from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
import sys
from datetime import datetime
import pandas as pd
import os

from tabulate import tabulate

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "social_network_document_database"

# -- shell comand to start mongod --
# mongod --dbpath /Volumes/ZX20/NoSQL_Project/data --logpath /Volumes/ZX20/NoSQL_Project/log/logmongodb.log --fork

class MongoDBManager:
    """Manages MongoDB connection and queries"""

    def __init__(self, connection_string = "mongodb://localhost:27017/", db_name = "social_network_document_database"):
        """ Initializes database connection """
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            print(f"Connected to database '{db_name}'")
        except Exception as e:
            print(f"Connection error: {e}")
            sys.exit(1)


    def load_data(self, file_path):
        """Loads data from CSV file into MongoDB"""

        collection_name = os.path.splitext(os.path.basename(file_path))[0]
        df = pd.read_csv(file_path, keep_default_na=False, sep="|")

        # cast the id values into to string
        for column in df.columns:
            if "id" in column.lower():
                df[column] = df[column].astype(str)

        data = df.to_dict("records")

        self.db[collection_name].insert_many(data)
        print(f"Inserted {len(data)} documents in collection '{collection_name}'")


    def get_person_info(self, person_id):
        """ get info about a person: firstName, lastName, gender, locationCity, birthday """

        # Check if person exists
        if not self.db['Person'].find_one({"id": person_id}, {"_id": 1}):
            return {"error": f"Person with ID {person_id} not found"}

        person = self.db['Person'].find_one({"id": person_id})
        locationCity = self.db['Place'].find_one({"id": person["LocationCityId"]})

        person_info = {
            "firstName": person["firstName"],
            "lastName": person["lastName"],
            "gender": person["gender"],
            "birthday": person["birthday"],
            "locationCity": locationCity["name"] if locationCity else ""
        }


        return person_info


    def get_person_locations(self, person_id):
        """
        Find university and company locations for a person.
        """

        # Check if person exists
        if not self.db['Person'].find_one({"id": person_id}, {"_id": 1}):
            return {"error": f"Person with ID {person_id} not found"}

        result = {"University": [], "Company": []}

        person_id = str(person_id)

        # retrive the university in relation to the person
        study_relations = list(self.db['Person_studyAt_University'].find(
            {"PersonId": person_id},
            {"UniversityId": 1}
        ))

        if study_relations:
            university_ids = [study["UniversityId"] for study in study_relations]

        else:
            university_ids = []

        print(f"university_ids: {university_ids}")

        # retrive the jobs in relation to the person
        work_relations = list(self.db['Person_workAt_Company'].find(
            {"PersonId": person_id},
            {"CompanyId": 1}
        ))

        if work_relations:
            company_ids = [work["CompanyId"] for work in work_relations]

        else:
            company_ids = []

        print(f"company_ids: {company_ids}")
        # build the list of organizations ids (University and Companies)
        organization_ids = university_ids + company_ids

        # retrive the organisations info
        organisations = self.db['Organisation'].find(
            {"id": {"$in": organization_ids}},
            {"id": 1, "name": 1, "LocationPlaceId": 1, "type": 1, "_id": 0}
        ).to_list()


        organization_map = {}
        place_ids = set()
        for org in organisations:
            place_ids.add(org["LocationPlaceId"])
            organization_map[org["id"]] = org

        # retrive the cities
        cities = {place["id"]: place for place in self.db['Place'].find(
            {"id": {"$in": list(place_ids)}},
            {"id": 1, "name": 1, "PartOfPlaceId": 1, "_id": 0}
        )}

        # retrive the counties
        country_ids = [country["PartOfPlaceId"] for country in cities.values() if "PartOfPlaceId" in country]
        countries = {place["id"]: place["name"] for place in self.db['Place'].find(
            {"id": {"$in": country_ids}},
            {"id": 1, "name": 1, "_id": 0}
        )}

        #build the result
        for org in organisations:
            org_type = org["type"]
            if org_type == "University":
                result[org_type].append({
                    "University": org["name"],
                    "City": cities[org["LocationPlaceId"]]["name"],
                    "Nation": countries[cities[org["LocationPlaceId"]]["PartOfPlaceId"]]
                })
            else:
                result[org_type].append({
                    "Company": org["name"],
                    "Nation": cities[org["LocationPlaceId"]]["name"],
                    "Continent": countries[cities[org["LocationPlaceId"]]["PartOfPlaceId"]]
                })


        return result

    def get_university_students(self, university_id, exclude_id = None):
        """
        get all students of a university exelcluding the person with id = exclude_id
        """

        # Check if the person exists
        if not self.db['Organisation'].find_one({"id": university_id, "type": "University"}, {"_id": 1}):
            return {"error": f"University with ID {university_id} not found"}

        if exclude_id:
            studyAt_relations = self.db['Person_studyAt_University'].find({"UniversityId": university_id, "PersonId": {"$ne": exclude_id}}, {"PersonId": 1})
        else:
            studyAt_relations = self.db['Person_studyAt_University'].find({"UniversityId": university_id}, {"PersonId": 1})

        result = [elem["PersonId"] for elem in studyAt_relations]

        return result

    def get_university_colleagues(self, person_id):
        """
        get all colleagues of a person in the university where they study
        """

        # Check if the person exists
        if not self.db['Person'].find_one({"id": person_id}, {"_id": 1}):
            return {"error": f"Person with ID {person_id} not found"}

        # retrive the university in relation to the person
        study_relations = self.db['Person_studyAt_University'].find(
            {"PersonId": person_id},
            {"UniversityId": 1}
        )
        if not study_relations:
            return []

        university_id = study_relations[0]["UniversityId"]

        colleagues = self.get_university_students(university_id, person_id)

        return colleagues

    def get_work_colleagues(self, person_id):
        """
        get all colleagues of a person in the company where they work at the moment (last work)
        """

        # Check if the person exists
        if not self.db['Person'].find_one({"id": person_id}, {"_id": 1}):
            return {"error": f"Person with ID {person_id} not found"}

        # retrive the most recent work relation to the person (actual or last work place)
        most_recent_work_relation = self.db['Person_workAt_Company'].find({"PersonId": person_id},
                                {"_id": 0, "CompanyId": 1, "workFrom": 1}).sort("workFrom", -1).limit(1)

        last_job = list(most_recent_work_relation)

        if not last_job:
            return []

        company_id = last_job[0]["CompanyId"]

        colleagues_relations = self.db['Person_workAt_Company'].find(
                    {"CompanyId": company_id},
                    {"_id": 0, "PersonId": 1}
        )

        colleagues = [c["PersonId"] for c in colleagues_relations]

        return colleagues



    def print_table(self, data):
        print(tabulate(data, headers="keys", tablefmt="psql"))

    def close(self):
        self.client.close()

if __name__ == "__main__":

    # Initialize manager
    manager = MongoDBManager(MONGO_URI, MONGO_DB)
    person_id = "14"
    university_id = "2206"

    #manager.load_data("/Volumes/ZX20/NoSQL_Project/ldbc_data/ldbc_output/graphs/csv/interactive/composite-merged-fk/static/Organisation.csv")
    result = manager.get_university_students(university_id)
    print(result)


    manager.close()
