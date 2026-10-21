# Databricks notebook source
# MAGIC %run ./Classroom-Setup-07

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating Volume 
# MAGIC CREATE VOLUME IF NOT EXISTS trigger_storage_location

# COMMAND ----------

copy_data()

# COMMAND ----------

import os
current_dir = os.getcwd()

# COMMAND ----------

def Demo_12_starter_job():
    
    import os
    from databricks.sdk.service.jobs import JobSettings as Job

    current_dir = os.getcwd()

    Demo_12 = Job.from_dict(
        {
            "name": f"Demo_12_Retail_Job_{my_catalog}",
            "tasks": [
                {
                    "task_key": "ingesting_customers",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 07 Files/7.1 - Creating customers table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "ingesting_orders",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 04 Files/4.1 - Creating orders table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "customers_orders_report",
                    "depends_on": [
                        {"task_key": "ingesting_orders"},
                        {"task_key": "ingesting_customers"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "ingesting_sales",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 04 Files/4.2 - Creating sales table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "customers_sales_summary",
                    "depends_on": [
                        {"task_key": "ingesting_customers"},
                        {"task_key": "ingesting_sales"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "checking_for_duplicates",
                    "depends_on": [
                        {"task_key": "customers_sales_summary"},
                    ],
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": "{{tasks.customers_sales_summary.values.has_duplicates}}",
                        "right": "true",
                    },
                },
                {
                    "task_key": "dropping_duplicate_records",
                    "depends_on": [
                        {"task_key": "checking_for_duplicates", "outcome": "true"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.3 - If Condition: Dropping Duplicates"),
                        "source": "WORKSPACE",
                    },
                    "environment_key": "Default",
                },
                {
                    "task_key": "transforming_customers_sales_table",
                    "depends_on": [
                        {"task_key": "checking_for_duplicates", "outcome": "false"},
                        {"task_key": "dropping_duplicate_records"},
                    ],
                    "run_if": "NONE_FAILED",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.4 - Else Condition: Cleaning and Transforming Customers Sales Table"),
                        "source": "WORKSPACE",
                    },
                    "environment_key": "Default",
                },
                {
                    "task_key": "customers_orders_state_wise_report_iterator",
                    "depends_on": [
                        {"task_key": "customers_orders_report"},
                    ],
                    "for_each_task": {
                        "inputs": "[\"CA\",\"NY\",\"VA\"]",
                        "concurrency": 3,
                        "task": {
                            "task_key": "customers_orders_state_wise_report",
                            "notebook_task": {
                                "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State"),
                                "base_parameters": {
                                    "state": "{{input}}",
                                },
                                "source": "WORKSPACE",
                            },
                            "environment_key": "Default",
                        },
                    },
                },
            ],
            "queue": {
                "enabled": True,
            },
            "parameters": [
                {
                    "name": "catalog",
                    "default": f"{my_catalog}",
                },
                {
                    "name": "schema",
                    "default": "jobs",
                },
            ],
            "environments": [
                {
                    "environment_key": "Default",
                    "spec": {
                        "environment_version": "5",
                    },
                },
            ],
            "performance_target": "PERFORMANCE_OPTIMIZED",
        }
    )
    
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    # create a new job using: 
    w.jobs.create(**Demo_12.as_shallow_dict())
    print(f"Created the Job Demo_12_Retail_Job_{my_catalog}")


# COMMAND ----------

def exporting_dashboard_from_input():
    import json
    import re
    import os

    # Path to the downloaded dashboard JSON
    input_file = os.path.join(
        current_dir,
        "Task Files/Lesson 12 Files/Input_file.json"
    )

    # Target catalog and schema
    current_catalog = my_catalog
    current_schema = "jobs"

    # Output dashboard filename
    output_file = os.path.join(
        current_dir,
        f"Task Files/Lesson 12 Files/{my_catalog}_Retail_Dashboard.json"
    )

    # Original catalog and schema used in the downloaded dashboard
    old_catalog = "dbacademy"
    old_schema = "myschema"

    # Matches: dbacademy.myschema.table_name
    # Group 1 = word boundary before the catalog
    # Group 2 = ".table_name"
    schema_pattern = re.compile(
        rf"(\b){re.escape(old_catalog)}\.{re.escape(old_schema)}(\.[a-zA-Z0-9_]+)"
    )

    def replace_schema(obj, pat, catalog, schema):
        if isinstance(obj, dict):
            return {
                key: replace_schema(value, pat, catalog, schema)
                for key, value in obj.items()
            }

        elif isinstance(obj, list):
            return [
                replace_schema(item, pat, catalog, schema)
                for item in obj
            ]

        elif isinstance(obj, str):
            return pat.sub(
                lambda match: f"{match.group(1)}{catalog}.{schema}{match.group(2)}",
                obj
            )

        return obj

    # Load the dashboard JSON
    with open(input_file, "r", encoding="utf-8") as f:
        dashboard_data = json.load(f)

    # Change dbacademy.myschema.<table> to my_catalog.jobs.<table>
    dashboard_data = replace_schema(
        dashboard_data,
        schema_pattern,
        current_catalog,
        current_schema
    )

    # Save the modified dashboard JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2)

    print(f"Updated dashboard exported to {output_file}")

# COMMAND ----------

def dashboard_creation_from_json():
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat
    from databricks.sdk.service.workspace import ObjectType
    import base64

    w = WorkspaceClient()

    # Read your JSON dashboard file
    with open(os.path.join(current_dir,f'Task Files/Lesson 12 Files/{my_catalog}_Retail_Dashboard.json'), 'r') as f:
        dashboard_content = f.read()

    # Base64 encode the content
    encoded_content = base64.b64encode(dashboard_content.encode()).decode()

    # Import the dashboard - path MUST end with .lvdash.json
    w.workspace.import_(
        path = os.path.join(current_dir,f'{my_catalog}_Retail_Dashboard.lvdash.json'),  # Critical: .lvdash.json extension
        format=ImportFormat.AUTO,  # Use ImportFormat.AUTO for automatic detection
        content=encoded_content,
        overwrite=True
    )
    dash_path = os.path.join(current_dir, f'{my_catalog}_Retail_Dashboard.lvdash.json')
    # Get workspace object status to read its object_id (resource_id)
    obj = w.workspace.get_status(path=dash_path) 
    dashboard_id = obj.object_id  

    # Publish the dashboard; set embed_credentials and optionally override warehouse
    w.lakeview.publish(
        dashboard_id=dashboard_id,
        embed_credentials=True,          
    )

    print("Retail Dashboard have been created successfully!\n")
    print(f"Your Dashboard name is {my_catalog}_Retail_Dashboard")

# COMMAND ----------

def dashboard_creation_from_input():
    exporting_dashboard_from_input()
    dashboard_creation_from_json()
