# Create Scripts

This directory contains scripts to create the necessary tables (Bronze, Silver, Gold) with the required schema.

## Table of Contents

- [create_bronze_table.py](#create_bronze_tablepy)
- [create_silver_table.py](#create_silver_tablepy)
- [create_gold_table.py](#create_gold_tablepy)

## create_bronze_table.py

### Functions

- `create_bronze_table(bronze_path)`: Creates the Bronze table with the required schema.

### Script Logic

- Defines the schema for the Bronze table.
- Creates an empty DataFrame with the schema.
- Writes the empty DataFrame to the Bronze path as a Delta table.

### Inputs

- `bronze_path`: Path to the Bronze layer.

### How to Run

- Run the script with the Bronze path as an argument.

## create_silver_table.py

### Functions

- `create_silver_table(silver_path)`: Creates the Silver table with the required schema.

### Script Logic

- Defines the schema for the Silver table.
- Creates an empty DataFrame with the schema.
- Writes the empty DataFrame to the Silver path as a Delta table.

### Inputs

- `silver_path`: Path to the Silver layer.

### How to Run

- Run the script with the Silver path as an argument.

## create_gold_table.py

### Functions

- `create_gold_table(gold_path)`: Creates the Gold table with the required schema.

### Script Logic

- Defines the schema for the Gold table.
- Creates an empty DataFrame with the schema.
- Writes the empty DataFrame to the Gold path as a Delta table.

### Inputs

- `gold_path`: Path to the Gold layer.

### How to Run

- Run the script with the Gold path as an argument.

