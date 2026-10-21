# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Test Runner — Single Course
# MAGIC
# MAGIC Creates a **single multi-task Lakeflow Job** for one course, with optional QA checks,
# MAGIC so the full run is visible in one timeline.
# MAGIC
# MAGIC **Run structure:**
# MAGIC - Course notebooks run in the dependency order you define in `COURSE_TASKS`.
# MAGIC - **QA Content Checker** can run independently (no dependencies) alongside the course tasks.
# MAGIC - The separate **Course-specific inputs** cell makes it easy to reuse this notebook for another course.
# MAGIC
# MAGIC ```
# MAGIC  QA Check      :  qa_content_checker  (optional, parallel)
# MAGIC  Course Tasks  :  task_01  ──►  task_02  ──►  task_03
# MAGIC ```
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Reads course-specific settings from a dedicated configuration cell (`COURSE_NAME`, `LAB_NOTEBOOKS`, `COURSE_TASKS`).
# MAGIC 2. Auto-fills all `<FILL_IN>` placeholders in the configured lab notebooks using the inline solution blocks.
# MAGIC 3. Resolves all notebook paths relative to this notebook's location.
# MAGIC 4. Creates (or re-creates) a single persistent Lakeflow Job for the configured course.
# MAGIC 5. Optionally adds `qa_content_checker` as an **independent task** (no dependencies).
# MAGIC 6. Triggers the job with `jobs.run_now` and polls until completion.
# MAGIC 7. Appends one result row per task to a generic results table.
# MAGIC 8. Displays a combined summary table with clickable `run_page_url` links.
# MAGIC 9. Creates and publishes a **Lakeview dashboard** from the run results and QA findings.
# MAGIC 10. Raises if any task failed — triggering Lakeflow Job failure email.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks with UI Instructions (Auto-Scanned)
# MAGIC
# MAGIC The following cell **dynamically scans** all course notebooks (demos and labs) to detect manual UI steps that cannot be fully automated by the test runner.
# MAGIC
# MAGIC It looks for markdown cells containing UI action patterns (e.g., "Select...", "Click...", "Navigate to...", icon references, etc.) and reports which notebooks have them.
# MAGIC
# MAGIC **This runs fresh on every execution** — no hardcoded values — so it always reflects the latest course content.

# COMMAND ----------

# MAGIC %run ./notebook_path

# COMMAND ----------

# ── Auto-Scan: Notebooks with UI Instructions ───────────────────────────────
# Dynamically scans ALL course notebooks (demos and labs) to detect cells
# containing manual UI steps that cannot be automated by the test runner.
#
# NOT HARDCODED — runs fresh every time so it reflects the latest course content.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# ── UI Detection Patterns ─────────────────────────────────────────────────────
# These regex patterns identify markdown cells that contain UI interaction steps.
# They match common instructional language used in Databricks Academy notebooks.

UI_ACTION_PATTERNS = [
    # Direct UI actions
    re.compile(r'\bSelect\s+(the\s+)?\*\*', re.IGNORECASE),          # "Select the **Catalog** icon"
    re.compile(r'\bClick\s+(the\s+|on\s+)?\*\*', re.IGNORECASE),    # "Click the **Run pipeline** button"
    re.compile(r'\bright-click\b', re.IGNORECASE),                    # "right-click on Jobs & Pipelines"
    re.compile(r'\bOpen\s+in\s+(a\s+)?(New|new)\s+Tab\b'),           # "Open in New Tab"
    re.compile(r'\bOpen\s+(Link\s+)?in\s+New\s+(Browser\s+)?Tab\b', re.IGNORECASE),
    # Navigation instructions
    re.compile(r'\b(left|top|main|far-left)\s+navigation\s+(bar|pane)\b', re.IGNORECASE),
    re.compile(r'\bNavigate\s+to\b', re.IGNORECASE),
    re.compile(r'\bExpand\s+(your|the)\s+\*\*', re.IGNORECASE),       # "Expand the **sdp_1_bronze** schema"
    # Icon references (images in markdown = UI screenshot)
    re.compile(r'!\[.*?Icon.*?\]\('),                                 # ![Catalog Icon](./path)
    re.compile(r'!\[.*?(icon|button|select|settings).*?\]\(', re.IGNORECASE),
    # Pipeline editor / Jobs & Pipelines UI
    re.compile(r'\bJobs\s*(&|and)\s*Pipelines\b', re.IGNORECASE),
    re.compile(r'\bLakeflow\s+(Pipelines?\s+)?Editor\b', re.IGNORECASE),
    re.compile(r'\bPipeline\s+(graph|details|settings)\b', re.IGNORECASE),
    re.compile(r'\bOpen\s+in\s+Editor\b', re.IGNORECASE),
    re.compile(r'\bRun\s+pipeline\b', re.IGNORECASE),
    re.compile(r'\bDry\s+Run\b', re.IGNORECASE),
    # Settings and configuration via UI
    re.compile(r'\bgear\s+icon\b', re.IGNORECASE),
    re.compile(r'\bellipsis\s+icon\b', re.IGNORECASE),
    re.compile(r'\bthree-dot\s+menu\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Create\*\*', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Settings\*\*', re.IGNORECASE),
    re.compile(r'\bAdd\s+configuration\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Save\*\*', re.IGNORECASE),
]

# Minimum number of distinct pattern matches in a cell to qualify as a UI step
MIN_PATTERN_MATCHES = 2


def extract_section_header(content: str) -> str:
    """Extract the markdown section header (##, ###) from a cell's content."""
    # Look for markdown headers
    match = re.search(r'^#{1,4}\s+(.+?)$', content, re.MULTILINE)
    if match:
        # Clean markdown formatting
        header = match.group(1).strip()
        header = re.sub(r'\*\*(.+?)\*\*', r'\1', header)  # Remove bold
        header = re.sub(r'!\[.*?\]\(.*?\)', '', header)    # Remove images
        return header.strip()
    return ""


def get_ui_step_summary(content: str) -> str:
    """Generate a short summary of what UI actions are described in the cell."""
    actions = []
    if re.search(r'\bSelect\s+(the\s+)?\*\*Catalog\*\*', content, re.IGNORECASE):
        actions.append("Navigate Catalog")
    if re.search(r'\bJobs\s*(&|and)\s*Pipelines\b', content, re.IGNORECASE):
        actions.append("Jobs & Pipelines")
    if re.search(r'\bRun\s+pipeline\b', content, re.IGNORECASE):
        actions.append("Run pipeline")
    if re.search(r'\bDry\s+Run\b', content, re.IGNORECASE):
        actions.append("Dry run")
    if re.search(r'\bOpen\s+in\s+Editor\b', content, re.IGNORECASE):
        actions.append("Open in Editor")
    if re.search(r'\bSchedule\b', content, re.IGNORECASE) and re.search(r'\bpipeline\b', content, re.IGNORECASE):
        actions.append("Schedule pipeline")
    if re.search(r'\bSettings\b', content) and re.search(r'\b(Select|gear|configure)\b', content, re.IGNORECASE):
        actions.append("Configure settings")
    if re.search(r'\bExpand\b', content, re.IGNORECASE) and re.search(r'\b(schema|catalog|volume|folder)\b', content, re.IGNORECASE):
        actions.append("Explore catalog/schema")
    if re.search(r'\bCreate\b.*\b(ETL|Pipeline)\b', content, re.IGNORECASE):
        actions.append("Create pipeline")
    if re.search(r'\bright-click\b', content, re.IGNORECASE):
        actions.append("Right-click menu")
    if re.search(r'\bPipeline\s+graph\b', content, re.IGNORECASE):
        actions.append("Pipeline graph")
    if re.search(r'\bevent.*log\b', content, re.IGNORECASE):
        actions.append("Event log")
    if re.search(r'\bEnable\b.*\b(Lakeflow|Editor|feature)\b', content, re.IGNORECASE):
        actions.append("Enable feature")
    return ", ".join(actions) if actions else "UI interaction"


def scan_notebook_for_ui_steps(notebook_path: str) -> list:
    """Scan a notebook for markdown cells containing UI instructions.
    
    Returns a list of dicts with section header and summary for each UI step found.
    """
    ui_steps = []
    
    try:
        export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
    except Exception as e:
        return [{"section": "ERROR", "summary": str(e)}]
    
    cells = nb_json.get("cells", [])
    
    for cell in cells:
        source = ''.join(cell.get("source", []))
        
        # Only check markdown cells and code cells with %md magic (used in Databricks)
        is_markdown = cell.get("cell_type") == "markdown"
        is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
        
        if not (is_markdown or is_md_magic):
            continue
        
        # Count how many distinct UI patterns match in this cell
        matched_patterns = sum(1 for p in UI_ACTION_PATTERNS if p.search(source))
        
        if matched_patterns >= MIN_PATTERN_MATCHES:
            section = extract_section_header(source)
            summary = get_ui_step_summary(source)
            # Avoid duplicate entries for the same section
            if not any(s["section"] == section and section for s in ui_steps):
                ui_steps.append({
                    "section": section or "(unlabeled section)",
                    "summary": summary,
                    "pattern_matches": matched_patterns,
                })
    
    return ui_steps


# ── Scan All Course Notebooks ─────────────────────────────────────────────────
# Only scan Demos and Labs (lectures are presentation-only, no UI steps expected)

print("═" * 70)
print("AUTO-SCAN: NOTEBOOKS WITH UI INSTRUCTIONS")
print("═" * 70)
print("\nScanning all Demo and Lab notebooks for UI instruction patterns...\n")

UI_INSTRUCTION_NOTEBOOKS = {}
total_scanned = 0

for task in COURSE_TASKS:
    notebook_name = task["name"]
    # Scan Demos and Labs (skip Lectures and overview/summary notebooks)
    if not any(keyword in notebook_name for keyword in ["Demo", "Lab"]):
        continue
    
    notebook_path = os.path.normpath(f"{course_root}/{task['relative_path']}")
    total_scanned += 1
    
    ui_steps = scan_notebook_for_ui_steps(notebook_path)
    
    if ui_steps:
        UI_INSTRUCTION_NOTEBOOKS[notebook_name] = ui_steps

# ── Display Results ───────────────────────────────────────────────────────────
print(f"{len(UI_INSTRUCTION_NOTEBOOKS)} of {total_scanned} Demo/Lab notebooks contain UI steps:\n")

for notebook_name, steps in UI_INSTRUCTION_NOTEBOOKS.items():
    print(f"📋 {notebook_name}  ({len(steps)} UI sections)")
    for step in steps:
        section = step['section']
        summary = step['summary']
        print(f"     • {section} — {summary}")
    print()

total_steps = sum(len(s) for s in UI_INSTRUCTION_NOTEBOOKS.values())
print("═" * 70)
print(f"SCANNED: {total_scanned} notebooks  |  WITH UI STEPS: {len(UI_INSTRUCTION_NOTEBOOKS)}  |  TOTAL UI SECTIONS: {total_steps}")
print("═" * 70)

# COMMAND ----------

# Catalog / schema / table names
RESULTS_CATALOG   = "dbacademy"
RESULTS_SCHEMA    = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
RESULTS_TABLE     = "notebook_run_results"
QA_FINDINGS_TABLE = "qa_content_findings"

# How long to wait for the entire job run (seconds)
TOTAL_RUN_TIMEOUT_SECONDS = 60 * 60 * 2   # 2 hour ceiling
POLL_INTERVAL_SECONDS     = 15

# Job and dashboard names
JOB_NAME       = f"[Course Validation] {COURSE_NAME}"
DASHBOARD_NAME = f"[Course Validation] {COURSE_NAME} Results Dashboard"

# Build the task list from the reusable course configuration above.
TASKS = []

if RUN_QA_CHECKER:
    TASKS.append({
        "task_key": "qa_content_checker",
        "course": "QA",
        "name": QA_TASK_NAME,
        "relative_path": QA_TASK_RELATIVE_PATH,
        "depends_on": [],
        "env_key": "qa_env",
    })

for task in COURSE_TASKS:
    TASKS.append({
        "task_key": task["task_key"],
        "course": COURSE_NAME,
        "name": task["name"],
        "relative_path": task["relative_path"],
        "depends_on": task.get("depends_on", []),
        "env_key": task["env_key"],
        "use_classic": task.get("use_classic", False),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and workspace client

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobEmailNotifications,
    JobEnvironment,
    NotebookTask,
    RunLifeCycleState,
    RunResultState,
    Task,
    TaskDependency,
)

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve workspace paths

# COMMAND ----------

this_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
course_root = "/".join(this_notebook_path.split("/")[:-1])

for t in TASKS:
    # os.path.normpath resolves '../' so Databricks gets a clean absolute path
    t["notebook_path"] = os.path.normpath(f"{course_root}/{t['relative_path']}")

print(f"Course root: {course_root}\n")
for t in TASKS:
    print(f"  [{t['task_key']}]  {t['name']}\n      {t['notebook_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure results table exists

# COMMAND ----------

results_schema_name = RESULTS_SCHEMA if RESULTS_SCHEMA != "information_schema" else "default"
results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"

results_schema = StructType([
    StructField("run_timestamp",    TimestampType(), nullable=False),
    StructField("job_id",           LongType(),      nullable=True),
    StructField("job_run_id",       LongType(),      nullable=True),
    StructField("course",           StringType(),    nullable=True),
    StructField("task_key",         StringType(),    nullable=False),
    StructField("demo_name",        StringType(),    nullable=False),
    StructField("notebook_path",    StringType(),    nullable=False),
    StructField("status",           StringType(),    nullable=False),  # PASS / FAIL / TIMEOUT
    StructField("result_state",     StringType(),    nullable=True),
    StructField("life_cycle_state", StringType(),    nullable=True),
    StructField("duration_seconds", DoubleType(),    nullable=True),
    StructField("run_id",           LongType(),      nullable=True),
    StructField("run_page_url",     StringType(),    nullable=True),
    StructField("error_message",    StringType(),    nullable=True),
])

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
except Exception as e:
    if "UNAUTHORIZED_ACCESS" in str(e) or "PERMISSION_DENIED" in str(e):
        # Fall back to user's own labuser catalog (derived from username)
        user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
        print(f"⚠️  No permission to create schema in '{RESULTS_CATALOG}'. Falling back to catalog: {user_catalog}")
        RESULTS_CATALOG = user_catalog
        results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
    else:
        raise

if not spark.catalog.tableExists(results_fqn):
    spark.createDataFrame([], results_schema).write.format("delta").saveAsTable(results_fqn)
    print(f"Created results table: {results_fqn}")
else:
    print(f"Results table exists: {results_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create (or re-create) the course Lakeflow Job

# COMMAND ----------

# Delete any existing job with the same name so we always get a clean definition.
ENVIRONMENT_VERSION = "5"

existing = [j for j in w.jobs.list(name=JOB_NAME)]
for j in existing:
    w.jobs.delete(job_id=j.job_id)
    print(f"Deleted existing job: {j.job_id} ({j.settings.name})")

# Build Task objects from TASKS config.
import re

def _safe_key(k):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', k)

# All tasks share one environment (identical spec — consolidates to stay within the 10-env API limit).
SHARED_ENV_KEY = "shared_env"

job_tasks = []
for t in TASKS:
    job_tasks.append(
        Task(
            task_key=_safe_key(t["task_key"]),
            description=t["name"],
            notebook_task=NotebookTask(notebook_path=t["notebook_path"]),
            environment_key=SHARED_ENV_KEY,
            depends_on=[TaskDependency(task_key=_safe_key(dep if isinstance(dep, str) else dep['task_key'])) for dep in t["depends_on"]],
        )
    )

# ── Route classic-compute tasks to the existing cluster ────────────────────────
# Dynamically fetch the user's classic cluster
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = current_user.split("@")[0]

CLASSIC_CLUSTER_ID = None
for c in w.clusters.list():
    if c.creator_user_name == current_user or c.cluster_name == user_prefix:
        CLASSIC_CLUSTER_ID = c.cluster_id
        print(f"Found classic cluster: {c.cluster_name} (ID: {c.cluster_id})")
        break

if not CLASSIC_CLUSTER_ID:
    print("⚠️  No classic cluster found for user. Classic-compute tasks will fall back to serverless.")

if CLASSIC_CLUSTER_ID:
    for i, t in enumerate(TASKS):
        if t.get("use_classic"):
            job_tasks[i].environment_key = None
            job_tasks[i].existing_cluster_id = CLASSIC_CLUSTER_ID

job_environments = [
    JobEnvironment(
        environment_key=SHARED_ENV_KEY,
        spec=Environment(environment_version=ENVIRONMENT_VERSION),
    )
]

created_job = w.jobs.create(
    name=JOB_NAME,
    tasks=job_tasks,
    environments=job_environments,
    email_notifications=JobEmailNotifications(
        on_failure=TESTER_EMAILS,
        on_success=TESTER_EMAILS,
    ),
)
job_id = created_job.job_id
print(f"Created Lakeflow Job: {job_id}  ({JOB_NAME})")
print(f"\nTask DAG:")
for t in TASKS:
    deps = " → depends on: " + ", ".join((dep if isinstance(dep, str) else dep["task_key"]) for dep in t["depends_on"]) if t["depends_on"] else " (starts immediately)"
    compute_label = "[CLASSIC]" if t.get("use_classic") else "[SERVERLESS]"
    print(f"  {compute_label} {t['task_key']}{deps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patch Demo Notebooks for Automated Job Execution
# MAGIC
# MAGIC This course's demos instruct students to create and run **Lakeflow Jobs via the UI**.
# MAGIC For automated testing, we patch each demo notebook to insert cells that
# MAGIC programmatically create, configure, and run the jobs so SQL verification
# MAGIC queries at the end of each demo succeed.
# MAGIC
# MAGIC **Patches applied:**
# MAGIC - **04 Demo** — Insert cell to create & run the 2-task job (creates `orders_bronze`, `sales_bronze`)
# MAGIC - **05 Lab** — Insert cell to create & run the 3-task bank job (creates `bank_master_data_bronze`, `borrower_details_silver`, `loan_details_silver`)
# MAGIC - **07 Demo** — Insert cell to add `ingesting_customers` task with parameters & run the job (creates `customers_bronze`)
# MAGIC - **09 Demo** — Insert cell to fix task dependencies, add if-else + for-each tasks & run (creates gold/silver tables)
# MAGIC - **10 Lab** — Insert cell to add if-else processing tasks & run (creates `high_risk_borrowers_silver`)
# MAGIC - **12 Demo** — Fix intentional bug in `12.1` notebook; insert cell to run the Demo 12 job programmatically
# MAGIC - **15 Lab** — Insert cell to run the Lab 15 job programmatically after `lesson_15_starter_job()`

# COMMAND ----------

# ── Patch 04 Demo Notebook: Insert Job Creation + Run Cell ────────────────
# The 04 Demo has UI instructions to create and run a job manually.
# For automated testing, we insert or update a cell after print_job_config
# that programmatically creates the job and runs it, so the verification
# SQL queries (sales_bronze, orders_bronze) succeed.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

_this_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_parts = _this_path.split("/")
_includes_idx = next(i for i, p in enumerate(_parts) if p == "Includes")
_course_content_root = "/".join(_parts[:_includes_idx])

def _find_cell(cells, marker):
    for i, cell in enumerate(cells):
        if marker in "".join(cell.get("source", [])):
            return i
    return None

def _make_cell(code, comment="AUTO-INSERTED"):
    lines = [f"# {comment}\n"] + [line + "\n" for line in code.strip().split("\n")]
    return {"cell_type": "code", "source": lines, "metadata": {}, "outputs": [], "execution_count": None}

demo_04_path = _course_content_root + "/04 Demo - Creating a Job Using the Lakeflow Jobs UI"
export_resp = w.workspace.export(path=demo_04_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

marker_idx = _find_cell(cells, "print_job_config(catalog_name=")
existing_idx = _find_cell(cells, "# AUTO-INSERTED: Create and run Demo 04 job")

auto_insert_comment = "AUTO-INSERTED: Create and run Demo 04 job"
code_04 = '''import time
from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Demo_04_Retail_Job_{my_catalog}"

# Delete any existing job with the same name so reruns are idempotent
for _j in list(_w.jobs.list(name=_job_name)):
    _w.jobs.delete(job_id=_j.job_id)
    print(f"Deleted existing job: {_j.job_id} ({_j.settings.name})")

# Create the Demo 04 job programmatically (normally done via UI)
job_tasks = [
    {'task_name': 'ingesting_orders', 'file_path': '/Task Files/Lesson 04 Files/4.1 - Creating orders table', 'depends_on': None},
    {'task_name': 'ingesting_sales', 'file_path': '/Task Files/Lesson 04 Files/4.2 - Creating sales table', 'depends_on': None}
]
myjob = create_job_config(job_name=_job_name, job_tasks=job_tasks, job_parameters=[])

# Run the job and wait for completion
_run = _w.jobs.run_now(job_id=myjob.job_id)
_run_id = _run.run_id
print(f"Triggered Demo 04 job run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _state = _ri.state.life_cycle_state.value
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_result = _ri.state.result_state.value if _ri.state.result_state else "UNKNOWN"
print(f"Demo 04 job completed: {_result}")
assert _result == "SUCCESS", f"Demo 04 job failed: {_result}"'''

new_cell = _make_cell(code_04, auto_insert_comment)

if existing_idx is not None:
    cells[existing_idx] = new_cell
    action = f"updated existing auto-inserted cell at index {existing_idx}"
elif marker_idx is not None:
    cells.insert(marker_idx + 1, new_cell)
    action = f"inserted job creation + run cell after index {marker_idx}"
else:
    action = None
    print("\u26a0\ufe0f 04 Demo: print_job_config marker not found")

if action:
    nb_json["cells"] = cells
    modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(path=demo_04_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
    print(f"\u2705 Patched 04 Demo: {action}")

# COMMAND ----------

# ── Patch 05 Lab Notebook: Insert Job Creation + Run Cell ────────────────
# The 05 Lab has UI instructions to create a 3-task bank job manually.
# For automated testing, we insert a cell after print_job_config that
# programmatically creates the job and runs it, so the verification
# SQL queries (bank_master_data_bronze, borrower_details_silver,
# loan_details_silver) succeed.
# ─────────────────────────────────────────────────────────────────────────────

lab_05_path = _course_content_root + "/05 Lab - Create your First Job"
export_resp = w.workspace.export(path=lab_05_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

marker_idx = _find_cell(cells, "print_job_config(catalog_name=")
existing_idx = _find_cell(cells, "# AUTO-INSERTED: Create and run Lab 05 job")

auto_insert_comment = "AUTO-INSERTED: Create and run Lab 05 job"
code_05 = '''import time
from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Lab_05_Bank_Job_{my_catalog}"

# Delete any existing job with the same name so reruns are idempotent
for _j in list(_w.jobs.list(name=_job_name)):
    _w.jobs.delete(job_id=_j.job_id)
    print(f"Deleted existing job: {_j.job_id} ({_j.settings.name})")

# Create the Lab 05 job programmatically (normally done via UI)
job_tasks = [
    {"task_name": "ingesting_master_data", "file_path": "/Task Files/Lesson 05 Files/5.1 - Ingesting Banking Data", "depends_on": None},
    {"task_name": "creating_borrower_details_table", "file_path": "/Task Files/Lesson 05 Files/5.2 - Creating Borrower Details Table", "depends_on": [{"task_key": "ingesting_master_data"}]},
    {"task_name": "creating_loan_details_table", "file_path": "/Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table", "depends_on": [{"task_key": "ingesting_master_data"}]}
]
myjob = create_job_config(job_name=_job_name, job_tasks=job_tasks, job_parameters=[])

# Run the job and wait for completion
_run = _w.jobs.run_now(job_id=myjob.job_id)
_run_id = _run.run_id
print(f"Triggered Lab 05 job run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _state = _ri.state.life_cycle_state.value
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_result = _ri.state.result_state.value if _ri.state.result_state else "UNKNOWN"
print(f"Lab 05 job completed: {_result}")
assert _result == "SUCCESS", f"Lab 05 job failed: {_result}"'''

new_cell = _make_cell(code_05, auto_insert_comment)

if existing_idx is not None:
    cells[existing_idx] = new_cell
    action = f"updated existing auto-inserted cell at index {existing_idx}"
elif marker_idx is not None:
    cells.insert(marker_idx + 1, new_cell)
    action = f"inserted job creation + run cell after index {marker_idx}"
else:
    action = None
    print("\u26a0\ufe0f 05 Lab: print_job_config marker not found")

if action:
    nb_json["cells"] = cells
    modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(path=lab_05_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
    print(f"\u2705 Patched 05 Lab: {action}")

# COMMAND ----------

# ── Patch 07 Demo Notebook: Insert Cleanup + Job Update + Run Cell ──────
# The 07 Demo creates a starter job (2 tasks) but then asks students to
# add a customers task with parameters via UI and configure file arrival trigger.
# We insert:
#   1. A cleanup cell BEFORE create_job_config to delete any existing Demo_07 job
#   2. A cell after copy_data() that adds the customers task and runs the job.
# ─────────────────────────────────────────────────────────────────────────────

demo_07_path = _course_content_root + "/07 Demo - Automating Workloads with Scheduling and Triggers"
export_resp = w.workspace.export(path=demo_07_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

# ── Step 1: Insert cleanup cell before starter job creation ──────────────
starter_marker = 'create_job_config(job_name=f"Demo_07'
starter_idx = _find_cell(cells, starter_marker)
cleanup_idx = _find_cell(cells, "# AUTO-INSERTED: Delete existing Demo 07 job")

cleanup_comment_07 = "AUTO-INSERTED: Delete existing Demo 07 job"
cleanup_code_07 = '''from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Demo_07_Retail_Job_{my_catalog}"

for _j in list(_w.jobs.list(name=_job_name)):
    _w.jobs.delete(job_id=_j.job_id)
    print(f"Deleted existing job: {_j.job_id} ({_j.settings.name})")'''

if starter_idx is not None:
    new_cleanup_cell = _make_cell(cleanup_code_07, cleanup_comment_07)
    if cleanup_idx is not None:
        cells[cleanup_idx] = new_cleanup_cell
    else:
        cells.insert(starter_idx, new_cleanup_cell)
    print(f"\u2705 Inserted/updated cleanup cell before starter job creation")
else:
    print("\u26a0\ufe0f  07 Demo: create_job_config marker not found — skipping cleanup insertion")

# ── Step 2: Insert job update + run cell after copy_data() ───────────────
marker_idx = _find_cell(cells, "copy_data()")
existing_idx = _find_cell(cells, "# AUTO-INSERTED: Add customers task and run Demo 07 job")
auto_insert_comment_07 = "AUTO-INSERTED: Add customers task and run Demo 07 job"
code_07 = '''import time, os
from databricks.sdk import WorkspaceClient as _WC
from databricks.sdk.service.jobs import JobSettings, Task, NotebookTask

_w = _WC()
_course_path = os.getcwd()

# Update the starter job to add ingesting_customers task with parameters
_current = _w.jobs.get(job_id=myjob.job_id)
_tasks = list(_current.settings.tasks)
_tasks.append(
    Task(
        task_key="ingesting_customers",
        notebook_task=NotebookTask(
            notebook_path=_course_path + "/Task Files/Lesson 07 Files/7.1 - Creating customers table",
            base_parameters={"catalog": my_catalog, "schema": "jobs"}
        )
    )
)
_w.jobs.reset(
    job_id=myjob.job_id,
    new_settings=JobSettings(
        name=_current.settings.name,
        tasks=_tasks,
        parameters=_current.settings.parameters
    )
)
print(f"Updated job {myjob.job_id} with ingesting_customers task")

# Run the job and wait
_run = _w.jobs.run_now(job_id=myjob.job_id)
_run_id = _run.run_id
print(f"Triggered Demo 07 job run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _state = _ri.state.life_cycle_state.value
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_result = _ri.state.result_state.value if _ri.state.result_state else "UNKNOWN"
print(f"Demo 07 job completed: {_result}")
assert _result == "SUCCESS", f"Demo 07 job failed: {_result}"'''

new_cell_07 = _make_cell(code_07, auto_insert_comment_07)
if existing_idx is not None:
    cells[existing_idx] = new_cell_07
    action_07 = f"updated existing auto-inserted cell at index {existing_idx}"
elif marker_idx is not None:
    cells.insert(marker_idx + 1, new_cell_07)
    action_07 = f"inserted job update + run cell after index {marker_idx}"
else:
    action_07 = None
    print("\u26a0\ufe0f 07 Demo: copy_data() marker not found")

if action_07:
    nb_json["cells"] = cells
    modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(path=demo_07_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
    print(f"\u2705 Patched 07 Demo: {action_07}")

# COMMAND ----------

# ── Patch 09 Demo Notebook: Build Full Job DAG + Run ────────────────────
# The 09 Demo creates a 5-task starter job but then asks students to
# add dependencies, if-else conditions, and for-each loops via UI.
# We insert a cleanup cell before job creation, then a cell that rebuilds
# the job with the complete DAG and runs it.
# ─────────────────────────────────────────────────────────────────────────────

demo_09_path = _course_content_root + "/09 Demo - Building Dynamic Workloads with Advanced Tasks"
export_resp = w.workspace.export(path=demo_09_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

marker_text_09 = 'create_job_config(job_name=f"Demo_09'
marker_idx = _find_cell(cells, marker_text_09)
cleanup_idx = _find_cell(cells, "# AUTO-INSERTED: Delete existing Demo 09 job")
existing_idx = _find_cell(cells, "# AUTO-INSERTED: Build full Demo 09 job DAG and run")

cleanup_comment_09 = "AUTO-INSERTED: Delete existing Demo 09 job"
auto_insert_comment_09 = "AUTO-INSERTED: Build full Demo 09 job DAG and run"

cleanup_code_09 = '''from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Demo_09_Retail_Job_{my_catalog}"

for _j in list(_w.jobs.list(name=_job_name)):
    _w.jobs.delete(job_id=_j.job_id)
    print(f"Deleted existing job: {_j.job_id} ({_j.settings.name})")'''

code_09 = '''import time, os
from databricks.sdk import WorkspaceClient as _WC
from databricks.sdk.service.jobs import (
    JobSettings, Task, NotebookTask, TaskDependency,
    ConditionTask, ConditionTaskOp, ForEachTask, JobParameterDefinition, RunIf
)

_w = _WC()
_course_path = os.getcwd()


def _enum_value(x):
    return x.value if hasattr(x, "value") else x


# Rebuild Demo 09 job with full DAG: dependencies + if-else + for-each
_current = _w.jobs.get(job_id=myjob.job_id)

_full_tasks = [
    Task(task_key="ingesting_orders",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 04 Files/4.1 - Creating orders table")),
    Task(task_key="ingesting_sales",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 04 Files/4.2 - Creating sales table")),
    Task(task_key="ingesting_customers",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 07 Files/7.1 - Creating customers table",
                                     base_parameters={"catalog": my_catalog, "schema": "jobs"})),
    Task(task_key="customers_sales_summary",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table"),
         depends_on=[TaskDependency(task_key="ingesting_customers"), TaskDependency(task_key="ingesting_sales")]),
    Task(task_key="customers_orders_report",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table"),
         depends_on=[TaskDependency(task_key="ingesting_orders"), TaskDependency(task_key="ingesting_customers")]),
    Task(task_key="checking_for_duplicates",
         condition_task=ConditionTask(
             left="{{tasks.customers_sales_summary.values.has_duplicates}}",
             op=ConditionTaskOp.EQUAL_TO,
             right="true"
         ),
         depends_on=[TaskDependency(task_key="customers_sales_summary")]),
    Task(task_key="dropping_duplicate_records",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 09 Files/9.3 - If Condition: Dropping Duplicates"),
         depends_on=[TaskDependency(task_key="checking_for_duplicates", outcome="true")]),
    Task(task_key="transforming_customers_sales_table",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 09 Files/9.4 - Else Condition: Cleaning and Transforming Customers Sales Table"),
         depends_on=[
             TaskDependency(task_key="checking_for_duplicates", outcome="false"),
             TaskDependency(task_key="dropping_duplicate_records")
         ],
         run_if=RunIf.NONE_FAILED),
    Task(task_key="customers_orders_state_wise_report_iterator",
         for_each_task=ForEachTask(
             inputs='["CA", "NY", "VA"]',
             task=Task(
                 task_key="customers_orders_state_wise_report",
                 notebook_task=NotebookTask(
                     notebook_path=_course_path + "/Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State",
                     base_parameters={"state": "{{input}}"}
                 )
             )
         ),
         depends_on=[TaskDependency(task_key="customers_orders_report")]),
]

# Fix SDK serialization bug on serverless env: Task.as_dict() calls
# self.run_if.value but the SDK __init__ may store it as a string.
for _t in _full_tasks:
    if hasattr(_t, 'run_if') and isinstance(_t.run_if, str):
        _t.run_if = RunIf(_t.run_if)

_w.jobs.reset(
    job_id=myjob.job_id,
    new_settings=JobSettings(
        name=_current.settings.name,
        tasks=_full_tasks,
        parameters=[
            JobParameterDefinition(name="catalog", default=my_catalog),
            JobParameterDefinition(name="schema", default="jobs")
        ]
    )
)
print("Updated Demo 09 job with full DAG (if-else + for-each)")

# Run the job and wait
_run = _w.jobs.run_now(job_id=myjob.job_id)
_run_id = _run.run_id
print(f"Triggered Demo 09 job run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _state = _enum_value(_ri.state.life_cycle_state)
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_result = _enum_value(_ri.state.result_state) if _ri.state.result_state is not None else "UNKNOWN"
print(f"Demo 09 job completed: {_result}")
assert _result == "SUCCESS", f"Demo 09 job failed: {_result}"'''

if marker_idx is None:
    print("\u26a0\ufe0f 09 Demo: create_job_config marker not found")
else:
    new_cleanup_cell_09 = _make_cell(cleanup_code_09, cleanup_comment_09)
    new_run_cell_09 = _make_cell(code_09, auto_insert_comment_09)

    if cleanup_idx is not None:
        cells[cleanup_idx] = new_cleanup_cell_09
    else:
        cells.insert(marker_idx, new_cleanup_cell_09)

    marker_idx = _find_cell(cells, marker_text_09)

    if existing_idx is not None:
        cells[existing_idx] = new_run_cell_09
        action_09 = f"updated existing auto-inserted cell at index {existing_idx}"
    else:
        cells.insert(marker_idx + 1, new_run_cell_09)
        action_09 = "inserted cleanup + full job DAG + run cells"

    nb_json["cells"] = cells
    modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(path=demo_09_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
    print(f"\u2705 Patched 09 Demo: {action_09}")

# COMMAND ----------

# ── Patch 10 Lab Notebook: Add If-Else Tasks + Run Job ──────────────────
# The 10 Lab creates a starter job (3 tasks from Lesson 05) but then asks
# students to add if-else conditional tasks via the UI.
# We insert a cell that adds the if-else tasks and runs the job.
# ─────────────────────────────────────────────────────────────────────────────

lab_10_path = _course_content_root + "/10 Lab - Adding If-Else Task and Automating your Job"
export_resp = w.workspace.export(path=lab_10_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

marker_idx = _find_cell(cells, 'create_job_config(job_name=f"Lab_10')
existing_idx = _find_cell(cells, "# AUTO-INSERTED: Add if-else tasks and run Lab 10 job")
auto_insert_comment_10 = "AUTO-INSERTED: Add if-else tasks and run Lab 10 job"
code_10 = '''import time, os
from databricks.sdk import WorkspaceClient as _WC
from databricks.sdk.service.jobs import (
    JobSettings, Task, NotebookTask, TaskDependency,
    ConditionTask, ConditionTaskOp
)

_w = _WC()
_course_path = os.getcwd()

# Update Lab 10 job: add if-else conditional + processing tasks
_current = _w.jobs.get(job_id=myjob.job_id)
_tasks = list(_current.settings.tasks)
_tasks.extend([
    Task(task_key="checking_for_risky_borrowers",
         condition_task=ConditionTask(
             left="{{tasks.creating_loan_details_table.values.risk_flag}}",
             op=ConditionTaskOp.EQUAL_TO,
             right="true"
         ),
         depends_on=[TaskDependency(task_key="creating_loan_details_table")]),
    Task(task_key="processing_high_risk_borrowers",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers"),
         depends_on=[TaskDependency(task_key="checking_for_risky_borrowers", outcome="true")]),
    Task(task_key="processing_low_risk_borrowers",
         notebook_task=NotebookTask(notebook_path=_course_path + "/Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers"),
         depends_on=[TaskDependency(task_key="checking_for_risky_borrowers", outcome="false")]),
])

_w.jobs.reset(
    job_id=myjob.job_id,
    new_settings=JobSettings(
        name=_current.settings.name,
        tasks=_tasks,
        parameters=_current.settings.parameters or []
    )
)
print(f"Updated Lab 10 job with if-else + processing tasks")

# Run the job and wait
_run = _w.jobs.run_now(job_id=myjob.job_id)
_run_id = _run.run_id
print(f"Triggered Lab 10 job run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _state = _ri.state.life_cycle_state.value
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_result = _ri.state.result_state.value if _ri.state.result_state else "UNKNOWN"
print(f"Lab 10 job completed: {_result}")
assert _result == "SUCCESS", f"Lab 10 job failed: {_result}"'''

new_cell_10 = _make_cell(code_10, auto_insert_comment_10)
if existing_idx is not None:
    cells[existing_idx] = new_cell_10
    action_10 = f"updated existing auto-inserted cell at index {existing_idx}"
elif marker_idx is not None:
    cells.insert(marker_idx + 1, new_cell_10)
    action_10 = "inserted if-else tasks + run cell"
else:
    action_10 = None
    print("\u26a0\ufe0f 10 Lab: create_job_config marker not found")

if action_10:
    nb_json["cells"] = cells
    modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(path=lab_10_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
    print(f"\u2705 Patched 10 Lab: {action_10}")

# COMMAND ----------

# ── Patch 12 Demo: Fix Intentional Bug in 12.1 Notebook ─────────────────
# The 12.1 notebook has an intentional bug (wrong column name: 'customer'
# instead of 'customer_name') used to demonstrate job repair. For automated
# testing, we fix this bug so the job run succeeds without manual repair.
# ─────────────────────────────────────────────────────────────────────────────

nb_12_1_path = _course_content_root + "/Task Files/Lesson 12 Files/12.1 - Transforming Customers Orders State Wise Data"
try:
    export_resp = w.workspace.export(path=nb_12_1_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json["cells"]

    patched = False
    for cell in cells:
        source = "".join(cell.get("source", []))
        # Look for the clean_common function with the wrong column name
        if "def clean_common" in source or "clean_common" in source:
            # Fix: replace standalone 'customer' column reference with 'customer_name'
            # Only fix inside the function definition where it's clearly a column reference
            import re
            # Match 'customer' or "customer" as a standalone column reference (not part of 'customer_name')
            new_source = re.sub(
                r'''(['"])customer\1(?!_)''',
                r'\1customer_name\1',
                source
            )
            if new_source != source:
                cell["source"] = [new_source]
                patched = True
                break

    if patched:
        modified = base64.b64encode(json.dumps(nb_json).encode()).decode()
        w.workspace.import_(path=nb_12_1_path, content=modified, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
        print("\u2705 Patched 12.1 notebook: fixed 'customer' \u2192 'customer_name' column bug")
    else:
        print("\u2139\ufe0f 12.1 notebook: no 'customer' column bug found (may already be fixed)")
except Exception as e:
    print(f"\u26a0\ufe0f Could not patch 12.1 notebook: {e}")

print("\n" + "\u2550" * 70)
print("PATCHING COMPLETE")
print("\u2550" * 70)

# COMMAND ----------

# ── Patch 12 Demo: Insert Job Run Cell ───────────────────────────────────────
# The 12 Demo creates a job via Demo_12_starter_job() but relies on UI
# instructions to run it. We insert a cell after the starter call that
# programmatically runs the job so SQL verification queries succeed.
# ─────────────────────────────────────────────────────────────────────────────

demo_12_path = _course_content_root + "/12 Demo - Monitoring and Repairing Task"
try:
    export_resp = w.workspace.export(path=demo_12_path, format=ExportFormat.JUPYTER)
    nb_json_12 = json.loads(base64.b64decode(export_resp.content))
    cells_12 = nb_json_12["cells"]

    marker_12_idx = _find_cell(cells_12, "Demo_12_starter_job()")
    existing_12_idx = _find_cell(cells_12, "# AUTO-INSERTED: Run Demo 12 job")

    run_code_12 = '''import time
from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Demo_12_Retail_Job_{my_catalog}"

_jobs = list(_w.jobs.list(name=_job_name))
assert len(_jobs) > 0, f"Job '{_job_name}' not found"
_job_id = _jobs[0].job_id

_run = _w.jobs.run_now(job_id=_job_id)
_run_id = _run.run_id
print(f"Triggered {_job_name} run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _lcs = _ri.state.life_cycle_state
    _state = _lcs.value if hasattr(_lcs, "value") else str(_lcs)
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_rs = _ri.state.result_state
_result = _rs.value if hasattr(_rs, "value") else str(_rs) if _rs is not None else "UNKNOWN"
print(f"{_job_name} completed: {_result}")
assert _result == "SUCCESS", f"{_job_name} failed: {_result}"'''

    run_cell_12 = _make_cell(run_code_12, "AUTO-INSERTED: Run Demo 12 job")

    if marker_12_idx is not None:
        if existing_12_idx is not None:
            cells_12[existing_12_idx] = run_cell_12
            action_12 = f"updated existing run cell at index {existing_12_idx}"
        else:
            cells_12.insert(marker_12_idx + 1, run_cell_12)
            action_12 = f"inserted run cell after index {marker_12_idx}"

        nb_json_12["cells"] = cells_12
        modified_12 = base64.b64encode(json.dumps(nb_json_12).encode()).decode()
        w.workspace.import_(path=demo_12_path, content=modified_12, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
        print(f"\u2705 Patched 12 Demo: {action_12}")
    else:
        print("\u26a0\ufe0f 12 Demo: Demo_12_starter_job() marker not found")
except Exception as e:
    print(f"\u26a0\ufe0f Could not patch 12 Demo with run cell: {e}")

# ── Patch 15 Lab: Insert Job Run Cell ────────────────────────────────────────
# The 15 Lab creates a job via lesson_15_starter_job() but relies on UI
# instructions to run it. We insert a cell after the starter call that
# programmatically runs the job.
# ─────────────────────────────────────────────────────────────────────────────

lab_15_path = _course_content_root + "/15 BONUS LAB - Modular Orchestration"
try:
    export_resp = w.workspace.export(path=lab_15_path, format=ExportFormat.JUPYTER)
    nb_json_15 = json.loads(base64.b64decode(export_resp.content))
    cells_15 = nb_json_15["cells"]

    marker_15_idx = _find_cell(cells_15, "lesson_15_starter_job()")
    existing_15_idx = _find_cell(cells_15, "# AUTO-INSERTED: Run Lab 15 job")

    run_code_15 = '''import time
from databricks.sdk import WorkspaceClient as _WC

_w = _WC()
_job_name = f"Lab_15_Bank_Job_{my_catalog}"

_jobs = list(_w.jobs.list(name=_job_name))
assert len(_jobs) > 0, f"Job '{_job_name}' not found"
_job_id = _jobs[0].job_id

_run = _w.jobs.run_now(job_id=_job_id)
_run_id = _run.run_id
print(f"Triggered {_job_name} run: {_run_id} - waiting for completion...")
while True:
    _ri = _w.jobs.get_run(run_id=_run_id)
    _lcs = _ri.state.life_cycle_state
    _state = _lcs.value if hasattr(_lcs, "value") else str(_lcs)
    if _state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(15)
_rs = _ri.state.result_state
_result = _rs.value if hasattr(_rs, "value") else str(_rs) if _rs is not None else "UNKNOWN"
print(f"{_job_name} completed: {_result}")
assert _result == "SUCCESS", f"{_job_name} failed: {_result}"'''

    run_cell_15 = _make_cell(run_code_15, "AUTO-INSERTED: Run Lab 15 job")

    if marker_15_idx is not None:
        if existing_15_idx is not None:
            cells_15[existing_15_idx] = run_cell_15
            action_15 = f"updated existing run cell at index {existing_15_idx}"
        else:
            cells_15.insert(marker_15_idx + 1, run_cell_15)
            action_15 = f"inserted run cell after index {marker_15_idx}"

        nb_json_15["cells"] = cells_15
        modified_15 = base64.b64encode(json.dumps(nb_json_15).encode()).decode()
        w.workspace.import_(path=lab_15_path, content=modified_15, format=ImportFormat.JUPYTER, language=Language.PYTHON, overwrite=True)
        print(f"\u2705 Patched 15 Lab: {action_15}")
    else:
        print("\u26a0\ufe0f 15 Lab: lesson_15_starter_job() marker not found")
except Exception as e:
    print(f"\u26a0\ufe0f Could not patch 15 Lab with run cell: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger the job run and poll until all tasks complete

# COMMAND ----------

run_response  = w.jobs.run_now(job_id=job_id)
job_run_id    = run_response.run_id
run_timestamp = datetime.now(timezone.utc)

print(f"Triggered job run: {job_run_id}")
print(f"Polling every {POLL_INTERVAL_SECONDS}s (timeout={TOTAL_RUN_TIMEOUT_SECONDS}s)...\n")

deadline        = time.time() + TOTAL_RUN_TIMEOUT_SECONDS
final_run       = None
terminal_states = {RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR}

while time.time() < deadline:
    run = w.jobs.get_run(run_id=job_run_id)
    lc  = run.state.life_cycle_state if run.state else None

    task_states = {
        tk.task_key: (
            tk.state.life_cycle_state.value if tk.state and tk.state.life_cycle_state else "PENDING"
        )
        for tk in (run.tasks or [])
    }
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] run={lc}  tasks={task_states}")

    if lc in terminal_states:
        final_run = run
        break

    time.sleep(POLL_INTERVAL_SECONDS)
else:
    print("TIMEOUT — cancelling the run.")
    try:
        w.jobs.cancel_run(run_id=job_run_id)
    except Exception:
        pass
    final_run = w.jobs.get_run(run_id=job_run_id)

print(f"\nFinal run state: {final_run.state.life_cycle_state if final_run.state else 'UNKNOWN'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect per-task results

# COMMAND ----------

task_cfg = {t["task_key"]: t for t in TASKS}

results = []
for task_run in (final_run.tasks or []):
    cfg   = task_cfg.get(task_run.task_key, {})
    state = task_run.state

    result_state = state.result_state.value     if state and state.result_state     else None
    life_cycle   = state.life_cycle_state.value  if state and state.life_cycle_state  else None
    error_msg    = state.state_message           if state                             else None

    duration = None
    if task_run.start_time and task_run.end_time:
        duration = float((task_run.end_time - task_run.start_time) / 1000.0)

    if life_cycle == RunLifeCycleState.TERMINATED.value and result_state == RunResultState.SUCCESS.value:
        status = "PASS"
    elif life_cycle in (RunLifeCycleState.SKIPPED.value, RunLifeCycleState.INTERNAL_ERROR.value):
        status = "FAIL"
    elif life_cycle == RunLifeCycleState.TERMINATED.value:
        status = "FAIL"
    else:
        status = "TIMEOUT"

    results.append({
        "job_id":           job_id,
        "job_run_id":       job_run_id,
        "course":           cfg.get("course", ""),
        "task_key":         task_run.task_key,
        "demo_name":        cfg.get("name", task_run.task_key),
        "notebook_path":    cfg.get("notebook_path", ""),
        "status":           status,
        "result_state":     result_state,
        "life_cycle_state": life_cycle,
        "duration_seconds": duration,
        "run_id":           task_run.run_id,
        "run_page_url":     task_run.run_page_url,
        "error_message":    error_msg,
    })

# Sort back into configured TASKS order
order = {t["task_key"]: i for i, t in enumerate(TASKS)}
results.sort(key=lambda r: order.get(r["task_key"], 999))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append results to Delta + display summary

# COMMAND ----------

rows       = [Row(run_timestamp=run_timestamp, **r) for r in results]
results_df = spark.createDataFrame(rows, schema=results_schema)

results_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(results_fqn)

print(f"Appended {results_df.count()} rows to {results_fqn}\n")

# Summary for the configured single-course run
summary_groups = [COURSE_NAME]
if RUN_QA_CHECKER:
    summary_groups.append("QA")

for group_name in summary_groups:
    group_results = [r for r in results if r["course"] == group_name]
    if not group_results:
        continue

    passed = sum(1 for r in group_results if r["status"] == "PASS")
    failed = sum(1 for r in group_results if r["status"] != "PASS")
    icon = "✅" if failed == 0 else "❌"
    label = "QA Checks" if group_name == "QA" else COURSE_NAME
    print(f"  {icon} {label}:  {passed}/{len(group_results)} passed")

print(f"\nOverall: {sum(1 for r in results if r['status'] == 'PASS')}/{len(results)} tasks passed")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeview Dashboard

# COMMAND ----------

import json
from databricks.sdk.service.dashboards import Dashboard

# ── Dashboard Configuration ────────────────────────────────────────────────────
DASHBOARD_FOLDER = "/".join(this_notebook_path.split("/")[:-2])  # Up from CourseRunner/
qa_findings_fqn = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.{QA_FINDINGS_TABLE}"
# The QA checker may write to the 'default' schema — check both locations
if not spark.catalog.tableExists(qa_findings_fqn):
    qa_findings_alt = f"{RESULTS_CATALOG}.default.{QA_FINDINGS_TABLE}"
    if spark.catalog.tableExists(qa_findings_alt):
        qa_findings_fqn = qa_findings_alt
course_label_sql = COURSE_NAME.replace("'", "''")

# ── Dataset SQL (always filters to the latest run) ────────────────────────────
run_filter = f"job_run_id = (SELECT MAX(job_run_id) FROM {results_fqn})"
qa_filter  = f"run_timestamp = (SELECT MAX(run_timestamp) FROM {qa_findings_fqn})"

status_summary_sql = (
    f"SELECT status, COUNT(*) AS task_count "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"GROUP BY status ORDER BY status"
)

task_detail_sql = (
    f"SELECT CASE WHEN course = 'QA' THEN 'QA Checks' ELSE '{course_label_sql}' END AS run_group, "
    f"task_key, demo_name, status, ROUND(duration_seconds, 1) AS duration_seconds, "
    f"run_page_url, error_message "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"ORDER BY CASE WHEN course = 'QA' THEN 0 ELSE 1 END, task_key"
)

qa_sev_sql = (
    f"SELECT severity, COUNT(*) AS issue_count "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"GROUP BY severity ORDER BY issue_count DESC"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT 'No Data' AS severity, 0 AS issue_count WHERE 1=0"
)

qa_detail_sql = (
    f"SELECT notebook_name, cell_index, issue_type, severity, "
    f"offending_text, suggested_fix, check_source "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"ORDER BY severity, notebook_name, cell_index"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT '' AS notebook_name, 0 AS cell_index, '' AS issue_type, "
    "'' AS severity, '' AS offending_text, '' AS suggested_fix, '' AS check_source WHERE 1=0"
)

datasets = [
    {
        "name": "ds_status_summary",
        "displayName": "Task Status Summary",
        "query": status_summary_sql,
    },
    {
        "name": "ds_task_detail",
        "displayName": "Task Results Detail",
        "query": task_detail_sql,
    },
]

pages = [
    {
        "name": "pg_runs",
        "displayName": f"{COURSE_NAME} Run Results",
        "layout": [
            {
                "widget": {
                    "name": "w_bar",
                    "title": "Task Status — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_status_summary",
                            "fields": [
                                {"name": "status", "expression": "`status`"},
                                {"name": "task_count", "expression": "`task_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "status", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "task_count", "scale": {"type": "quantitative"}}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_task_table",
                    "title": "Task Results — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_task_detail",
                            "fields": [
                                {"name": "run_group", "expression": "`run_group`"},
                                {"name": "task_key", "expression": "`task_key`"},
                                {"name": "demo_name", "expression": "`demo_name`"},
                                {"name": "status", "expression": "`status`"},
                                {"name": "duration_seconds", "expression": "`duration_seconds`"},
                                {"name": "run_page_url", "expression": "`run_page_url`"},
                                {"name": "error_message", "expression": "`error_message`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "run_group"},
                                {"fieldName": "task_key"},
                                {"fieldName": "demo_name"},
                                {"fieldName": "status"},
                                {"fieldName": "duration_seconds"},
                                {"fieldName": "run_page_url"},
                                {"fieldName": "error_message"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    }
]

if RUN_QA_CHECKER:
    datasets.extend([
        {
            "name": "ds_qa_severity",
            "displayName": "QA Issues by Severity",
            "query": qa_sev_sql,
        },
        {
            "name": "ds_qa_detail",
            "displayName": "QA Findings Detail",
            "query": qa_detail_sql,
        },
    ])

    pages.append({
        "name": "pg_qa",
        "displayName": "QA Findings",
        "layout": [
            {
                "widget": {
                    "name": "w_qa_bar",
                    "title": "QA Issues by Severity — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_severity",
                            "fields": [
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "issue_count", "expression": "`issue_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "severity", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "issue_count", "scale": {"type": "quantitative"}}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_qa_table",
                    "title": "QA Findings Detail — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_detail",
                            "fields": [
                                {"name": "notebook_name", "expression": "`notebook_name`"},
                                {"name": "cell_index", "expression": "`cell_index`"},
                                {"name": "issue_type", "expression": "`issue_type`"},
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "offending_text", "expression": "`offending_text`"},
                                {"name": "suggested_fix", "expression": "`suggested_fix`"},
                                {"name": "check_source", "expression": "`check_source`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "notebook_name"},
                                {"fieldName": "cell_index"},
                                {"fieldName": "issue_type"},
                                {"fieldName": "severity"},
                                {"fieldName": "offending_text"},
                                {"fieldName": "suggested_fix"},
                                {"fieldName": "check_source"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    })

spec = {
    "datasets": datasets,
    "pages": pages,
}

# ── Auto-discover a SQL warehouse for the dashboard ───────────────────────────
from databricks.sdk.service.sql import State as WarehouseState

warehouse_id = None
try:
    for wh in w.warehouses.list():
        if wh.state in (WarehouseState.RUNNING, WarehouseState.STOPPED):
            warehouse_id = wh.id
            if wh.state == WarehouseState.RUNNING:
                break  # Prefer a running warehouse
except Exception as e:
    print(f"⚠️  Could not list warehouses: {e}")

if warehouse_id:
    print(f"Using SQL warehouse: {warehouse_id}")
else:
    print("⚠️  No SQL warehouse found — dashboard will have no data until one is assigned.")

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            w.lakeview.trash(dashboard_id=d.dashboard_id)
            print(f"Replaced existing dashboard: {d.dashboard_id}")
            break
except Exception:
    pass  # No existing dashboard — proceed to create

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
# Clean up: remove any existing dashboard (API-created or file-based)
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            state = str(d.lifecycle_state).upper() if d.lifecycle_state else ""
            if "TRASH" not in state:
                w.lakeview.trash(dashboard_id=d.dashboard_id)
                print(f"Replaced existing dashboard: {d.dashboard_id}")
except Exception:
    pass

# Also remove any .lvdash.json file with the same name
try:
    w.workspace.delete(path=f"{DASHBOARD_FOLDER}/{DASHBOARD_NAME}.lvdash.json")
except Exception:
    pass

dashboard = w.lakeview.create(Dashboard(
    display_name=DASHBOARD_NAME,
    serialized_dashboard=json.dumps(spec),
    parent_path=DASHBOARD_FOLDER,
    warehouse_id=warehouse_id,
))
dashboard_id = dashboard.dashboard_id
w.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=warehouse_id, embed_credentials=True)

workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
dashboard_url  = f"https://{workspace_host}/dashboardsv3/{dashboard_id}"

print(f"✅  Lakeview Dashboard created & published!")
print(f"    Name : {DASHBOARD_NAME}")
print(f"    ID   : {dashboard_id}")
print(f"    URL  : {dashboard_url}")
print(f"\n    ⚠️  NOTE: Lakeview API limitation — widget visualizations require one-time")
print(f"    activation via the dashboard editor. Open the dashboard and re-save widgets.")

# ── Inline Visualization (always works, regardless of dashboard rendering) ────
print("\n" + "═" * 70)
print("INLINE RESULTS VISUALIZATION")
print("═" * 70)

# Status summary chart
status_df = spark.sql(status_summary_sql)
display(status_df)

# QA severity chart (if table exists)
if spark.catalog.tableExists(qa_findings_fqn):
    qa_df = spark.sql(qa_sev_sql)
    display(qa_df)

# COMMAND ----------

# Display summary of failed tasks (deduplicated) with detailed error description
failed_rows = []
for r in results:
    if r["status"] != "PASS":
        name = r["demo_name"]
        if not any(row["Task"] == name for row in failed_rows):
            # Fetch detailed error from run output
            error_desc = ""
            try:
                run_output = w.jobs.get_run_output(run_id=r["run_id"])
                error_desc = (run_output.error or "").strip()
                if not error_desc and run_output.error_trace:
                    error_desc = run_output.error_trace.strip().split("\n")[-1]
            except Exception:
                pass
            failed_rows.append({
                "Task": name,
                "Error": r["error_message"] or "Workload failed",
                "Error Description": error_desc or "See run output for details",
            })

if failed_rows:
    failed_df = spark.createDataFrame(failed_rows)
    display(failed_df.select("Task", "Error", "Error Description"))
else:
    print("All tasks passed — no failures to report.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the notebook if any task failed

# COMMAND ----------

failed = [r for r in results if r["status"] != "PASS"]
if failed:
    summary = "\n".join(
        f"  - [{r['task_key']}] {r['demo_name']}: {r['status']} ({r['result_state']}) — {r['run_page_url']}"
        for r in failed
    )
    raise RuntimeError(
        f"{len(failed)} of {len(results)} task(s) failed on Serverless v{ENVIRONMENT_VERSION}:\n{summary}"
    )

print(f"All {len(results)} tasks passed on Serverless v{ENVIRONMENT_VERSION}.")
dbutils.notebook.exit(json.dumps({
    "total":  len(results),
    "passed": len(results),
    "failed": 0,
}))