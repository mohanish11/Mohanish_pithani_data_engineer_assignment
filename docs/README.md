# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

### Database Design

The database schema has been normalized into several related tables to properly separate concerns and maintain data integrity:

1. **Properties** - Core property information
   - Primary entity containing basic property details
   - Connected to other tables via foreign keys

2. **HOA** - Homeowners Association information
   - Linked to properties via property_id
   - Contains HOA-specific details

3. **Leads** - Lead information for properties
   - Connected to properties table
   - Tracks lead-related data

4. **Taxes** - Property tax information
   - Tax details linked to specific properties
   - Historical tax information

5. **Rehab** - Rehabilitation estimates and details
   - Connected to properties
   - Contains renovation and repair estimates

6. **Valuation** - Property valuation data
   - Linked to properties
   - Contains various valuation metrics

Each table uses appropriate data types and includes necessary constraints for data integrity.

### ETL Pipeline Design

The ETL pipeline is designed to run in parallel for optimal performance while maintaining data dependencies:

1. **Initial Data Load** (`load_raw.py`)
   - Reads the raw JSON data
   - Performs initial validation
   - Must run first

2. **Parallel Processing** (runs after initial load):
   - `property.py` - Processes core property data
   - `hoa.py` - Extracts and transforms HOA information
   - `leads.py` - Processes lead data
   - `taxes.py` - Handles tax information
   - `rehab.py` - Processes rehabilitation data
   - `valuation.py` - Handles valuation information

### Running the Solution

1. **Setup Environment:**
   ```bash
   # Create and activate virtual environment
   python3 -m venv venv
   source venv/bin/activate

   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Start the Database:**
   ```bash
   docker-compose -f docker-compose.initial.yml up --build -d
   ```

3. **Run the ETL Pipeline:**
   ```bash
   python run_etl.py
   ```

The ETL process will:
- First run the initial data load
- Then process all other transformations in parallel
- Log progress and any errors
- Show execution time

### Requirements

- Python 3.8 or higher
- Docker and Docker Compose
- Required Python packages (specified in requirements.txt):
   - mysql-connector-python

### Monitoring and Validation

- Check the logs for execution progress
- Each script logs its own progress
- Failed scripts are reported with specific error messages
- The pipeline stops if the initial load fails
- Parallel processing errors are captured and reported

### Error Handling

- Each script includes error handling and logging
- Database connection issues are properly handled
- Data validation errors are logged with details
- The pipeline reports overall success/failure status
