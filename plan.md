# Data Quality Improvement System - Plan

## What the system does
Takes structured raw data (CSV, JSON, XML, YAML, or database tables) as input, checks and improves its quality, then returns the cleaned data along with a report of all changes made.

---

## Step 1: Read the Input
- Accept files in CSV, JSON, XML, YAML formats, or a database connection
- Load the data into a common internal structure (like a table) regardless of the input format
- Handle basic read errors (file not found, bad format, empty file)

---

## Step 2: Understand the Data
- Detect the data type of each column automatically:
  - Is it a number (integer or float)?
  - Is it text (string)?
  - Is it a date?
  - Is it an email address?
  - Is it a phone number?
  - Is it a boolean (true/false)?
- Show the user a summary of what was detected before any changes are made

---

## Step 3: Check Data Quality
For each column, check for the following issues:
- Missing or empty values
- Wrong data type (e.g., text in a number column)
- Duplicate rows
- Inconsistent formats (e.g., dates written as "2024/01/15" vs "15-Jan-2024")
- Invalid values (e.g., negative age, badly formatted email)
- Extra whitespace or junk characters
- Inconsistent text casing (e.g., "new york" vs "New York")

---

## Step 4: Fix and Improve the Data
Apply fixes automatically where possible:
- Fill or flag missing values
- Standardize date formats to dd/mm/yyyy
- Normalize text (trim spaces, fix casing)
- Remove exact duplicate rows
- Correct data types where it is safe to do so
- Flag values that cannot be fixed automatically so the user is aware

---

## Step 5: Report the Changes
Generate a clear change report that tells the user:
- Which columns were affected
- What type of issue was found
- What was changed and what the new value is
- What could not be fixed and why

---

## Step 6: Output the Cleaned Data
- Return the cleaned data in the same format as the input (CSV in → CSV out, etc.)
- Also save the change report as a separate file (e.g., report.txt or report.json)

---

## Project Structure
```
claude-vsc/
├── plan.md
├── src/
│   ├── main.py          # Entry point — user runs this
│   ├── reader.py        # Reads CSV, JSON, XML, YAML, DB
│   ├── detector.py      # Detects column types (email, date, int, etc.)
│   ├── checker.py       # Finds quality issues in the data
│   ├── cleaner.py       # Fixes the issues found
│   ├── reporter.py      # Builds the change report
│   └── writer.py        # Writes cleaned data and report to output
```

---

## Order of Implementation
1. reader.py — get data in
2. detector.py — understand what kind of data it is
3. checker.py — find the problems
4. cleaner.py — fix the problems
5. reporter.py — explain what changed
6. writer.py — get data out
7. main.py — wire everything together
