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