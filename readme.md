# 🕸️ NoSQL Project
---

## 📖 Overview

This project implements simple platform designed to query social network database (generated from the [LDBC benchmark](https://github.com/ldbc/ldbc_snb_datagen_spark)). It utilizes two NoSQL databases:
* **MongoDB:** Handles static document data (Persons, Universities, Companies, Locations).
* **Neo4j:** Handles dynamic graph relationships (Knows, Likes, Created, HasTag).

---

## 📂 Project Structure

```text
andcamo-nosql_project/
├── main.py                 # Application entry point & API Controller
├── mongo_db_manager.py     # MongoDB connection and query logic
├── neo4j_manager.py        # Neo4j Driver connection and graph queries
├── requirements.txt        # Python dependencies
└── web/                    # Frontend assets
    ├── index.html          # Main UI
    ├── css/
    │   └── style.css       # Styling & Animations
    └── js/
        └── script.js       # Frontend logic & API calls
```
## 📕 Project Report
For more info check the **[📖 Relazione_Progetto_NoSQL.pdf](./Relazione_Progetto_NoSQL.pdf)**


## ⚙️ Configuration
The application requires local instances of MongoDB and Neo4j running. The code is pre-configured to connect via the following default credentials:

- MongoDB: mongodb://localhost:27017 (DB: social_network_document_database)

- Neo4j: bolt://localhost:7687 (User: neo4j, Pass: p4ssw0rd)

**Data Loading:** The file paths in the code currently point to the absolute paths of the generated data. To load your own data:

- Locate your LDBC dataset: Ensure you have the generated CSV files available locally.

- Update File Paths: Open mongo_db_manager.py and neo4j_manager.py and replace the hardcoded paths with the location of your local CSV files.

- Match Entities: Ensure the source CSV filenames correspond correctly to the entities being loaded (e.g., the script expects Person.csv to load Person nodes).

**Python Environment**: Ensure you have Python 3.8+ installed and the necessary dependencies provided in the requirements file.

Finally, run the application using the **main.py** file.