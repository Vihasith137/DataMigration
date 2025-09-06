### Oracle PL/SQL Data Migration & Mapping Framework
📌 Overview

This repository contains PL/SQL migration scripts, mapping logic, and automation templates developed for large-scale healthcare and public-sector data modernization projects.
The goal was to migrate legacy data into normalized Oracle schemas while ensuring data integrity, referential consistency, and audit compliance.

The framework includes:

PL/SQL migration scripts using MERGE, bulk processing, and error logging.

Data mapping logic aligning source staging schemas (STG_ODA) with normalized target schemas (LOC_DATA, PDS_DATA).

Automated audit triggers capturing INSERT/UPDATE/DELETE changes.

Error handling with DBMS_ERRLOG and exception logging.

Reference/junction table lookups (e.g., race, teeth_type, relationship_type) to satisfy FK constraints.

## ⚙️ Key Features

Assessment-driven migration: Parent-child table relationships (ASSESSMENT → CONTACT, DIAGNOSIS, SKILLED_SERVICE, etc.)

Data quality enforcement: Unknown defaults for mandatory fields, case-based mappings, and LIKE-based lookups.

Error logging: Centralized exception handling and row-level capture of failed inserts.

Auditability: Each target table has corresponding audit triggers and audit tables.

Performance optimization: Use of bulk MERGE, commit counters, and cursor-based loops where required.

## 🏗️ Technologies Used

Languages: Oracle PL/SQL, SQL

Databases: Oracle 12c/19c

Tools: Oracle SQL Developer, SQL*Plus, Data Modeler

Testing: UAT/SIT cycles with functional & regression validation
