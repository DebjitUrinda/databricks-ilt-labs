# Databricks notebook source
# DBTITLE 1,Catalog Setup
## Determines if in Vocareum or Other Workspace and sets up the catalog
## Usage: my_catalog = build_user_catalog() within your demo/lab setup.

import re
from typing import Optional

def _safe_uc_name(value: str) -> str:
    # UC identifiers are generally safest with letters, numbers, underscores
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "user"


def _current_user_email() -> str:
    """
    Get the user's name and email address.
    """
    return spark.sql("SELECT current_user()").first()[0]


def _get_workspace_catalogs():
    """
    Returns a set of Catalogs visible to that user.
    """
    list_of_catalogs_in_workspace = [row["catalog"].strip().lower() for row in spark.sql("SHOW CATALOGS").collect()]
    return list_of_catalogs_in_workspace


def _catalog_exists(name: str, catalogs: set[str]) -> bool:
    """
    Catalog checker to see if the catalog already exists for that user.
    """
    catalog_exists = name.lower() in catalogs
    return catalog_exists


def build_user_catalog(prefix: str = "labuser", catalog_forced = None) -> str:
    """
    Returns a UC catalog name for the current user.

    Parameters
    ----------
    prefix: str
        Prefix for the catalog name. Default is 'labuser'.
    catalog_forced: str
        Uses this catalog name if specified. Otherwise uses the prefix and user's name.

    Vocareum behavior:
      - If a catalog equals the user's 'labuserxxx' name and already exists,
        assume you are in Vocareum and use it.
      - Assumes users have a catalog by default in Vocareum.

    Other workspaces:
      - Use <prefix>_<user> and create it if possible for that user.

    Example:
        my_catalog = build_user_catalog(catalog_forced=None)  ## <-- Force the usage of a catalog if you can't create one.
    """

    # Obtain user's email and user name name
    user_email = _current_user_email()
    user_name = user_email.split("@")[0]

    # Make the user name safe if it's not in Vocareum
    safe_user_name = _safe_uc_name(user_name)


    # VOCAREUM CHECKER: Catalog is just the username (already provisioned)
    # and starts with 'labuser'
    vocareum_catalog_name = safe_user_name

    if user_email.lower().endswith("@vocareum.com"):
        print("✅ Vocareum workspace detected.")

        if _catalog_exists(
            name=vocareum_catalog_name,
            catalogs=_get_workspace_catalogs()
        ):
            print(f"✅ Using existing Vocareum catalog: '{vocareum_catalog_name}'.")
            return vocareum_catalog_name
        else:
            raise ValueError(
                f"❌ Catalog '{vocareum_catalog_name}' does not exist in this Vocareum workspace. "
                "Please create the catalog or verify the catalog name before continuing."
            )

    # OTHER WORKSPACE SETUP
    else:
        print("ℹ️ Non-Vocareum workspace detected. Setting up catalog.")

        # Setting catalog for workspaces outside of Vocareum using the provided prefix and user name
        # Limit the user's name to 19 characters. THis is done because there is a limit to the catalog.schema.object name (64 characters). For someone with a long name this could cause issus. Using 19 because that is the general size of the vocareum user name
        safe_user_name_char_restrict = safe_user_name[:19]

        # If catalog_forced is set, will use that by default.
        if catalog_forced is None:
            catalog_name = f"{prefix}_{safe_user_name_char_restrict}"
            print(f"ℹ️ Using default catalog name: '{catalog_name}'.")
        else:
            catalog_name = catalog_forced
            print(f"ℹ️ Using specified catalog name: '{catalog_name}'.")


        # Check if the user already has this catalog with the prefix_safeusername
        if _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == True:
            print(f"✅ Catalog '{catalog_name}' already exists. Using this catalog.")
            return catalog_name
        elif _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == False and catalog_forced is not None:
            raise RuntimeError(
                f"❌ Catalog '{catalog_name}' does not exist in this workspace. "
                "A forced catalog name must reference an existing catalog. "
                "Create the catalog or reference an existing one, then rerun the notebook."
            )
        else:
            try:
                print(f"ℹ️ Catalog '{catalog_name}' not found. Creating it now...")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
                print(f"✅ Catalog '{catalog_name}' created successfully.")
                return catalog_name
            except Exception as e:
                print(
                    f"⚠️ Could not create catalog '{catalog_name}'. "
                    "You may not have privileges to create catalogs in this workspace.\n"
                    f"Error: {e}"
                )


# COMMAND ----------

# DBTITLE 1,Volume Setup
def create_volume(in_catalog: str, in_schema: str, volume_name: str):
    '''
    Create a volume in the specified catalog.schema.
    '''
    print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
    r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')

# COMMAND ----------

# DBTITLE 1,Delete Source
def delete_source_files(source_files: str):
    """
    Deletes all files in the specified source volume.

    This function iterates through all the files in the given volume,
    deletes them, and prints the name of each file being deleted.

    Parameters:
    - source_files : str
        The path to the volume containing the files to delete. 
        Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
            Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

    Returns:
    - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.

    Example:
    - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
    """

    import os

    print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
    if os.path.exists(source_files):
        list_of_files = sorted(os.listdir(source_files))
    else:
        list_of_files = None

    if not list_of_files:  # Checks if the list is empty.
        print(f"No files found in {source_files}.\n")
    else:
        for file in list_of_files:
            file_to_delete = source_files + file
            print(f'Deleting file: {file_to_delete}')
            dbutils.fs.rm(file_to_delete)

# COMMAND ----------

# DBTITLE 1,Create Directory
import os

def create_directory_in_user_volume(user_default_volume_path: str, create_folders: list):
    '''
    Creates multiple (or single) directories in the specified volume path.

    Args:
    -------
        user_default_volume_path (str): The base directory path where the folders will be created. 
                                        You can use the default DA.paths.working_dir as the user's volume path.
        create_folders (list): A list of strings representing folder names to be created within the base directory.

    Returns:
    -------
        None: This function does not return any values but prints log information about the created directories.

    Example: 
    -------
    create_directory_in_user_volume(user_default_volume_path=DA.paths.working_dir, create_folders=['customers', 'orders', 'status'])
    '''
    
    print('----------------------------------------------------------------------------------------')
    for folder in create_folders:

        create_folder = f'{user_default_volume_path}/{folder}'

        if not os.path.exists(create_folder):
        # If it doesn't exist, create the directory
            dbutils.fs.mkdirs(create_folder)
            print(f'Creating folder: {create_folder}')

        else:
            print(f"Directory {create_folder} already exists. No action taken.")
        
    print('----------------------------------------------------------------------------------------\n')

# COMMAND ----------

def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
    '''
    Copy files from one location to another destination's volume.

    This method performs the following tasks:
      1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
      2. Verifies that the source directory has at least `n` files.
      3. Copies files from the source to the destination, skipping files already present at the destination.
      4. Pauses for `sleep` seconds after copying each file.
      5. Stops after copying `n` files or if all files are processed.
      6. Will print information on the files copied.
    
    Parameters
    - copy_from (str): The source directory where files are to be copied from.
    - copy_to (str): The destination directory where files will be copied to.
    - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
    - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.

    Returns:
    - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.

    Example:
    - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
           copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
           n=1)
    '''
    import os
    import time

    print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")

    ## List all files in the copy_from volume and sort the list
    list_of_files_to_copy = sorted(os.listdir(copy_from))
    total_files_in_copy_location = len(list_of_files_to_copy)

    ## Get a list of files in the source
    list_of_files_in_source = os.listdir(copy_to)

    assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."

    ## Looping counter
    counter = 1

    ## Load files if not found in the co
    for file in list_of_files_to_copy:
      if file.startswith('_'):
        pass
      else:
        ## If the file is found in the source, skip it with a note. Otherwise, copy file.
        if file in list_of_files_in_source:
          print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
        else:
          file_to_copy = f'{copy_from}{file}'
          copy_file_to = f'{copy_to}{file}'
          print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
          dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
          
          ## Sleep after load
          time.sleep(sleep) 

        ## Stop after n number of loops based on argument.
        if counter == n:
          break
        else:
          counter = counter + 1

# COMMAND ----------

def drop_tables(in_catalog: str, in_schema: list, dry_run: bool = False):
    """
    Drops all tables and views in the specified schema within a given catalog.

    Args:
        in_catalog (str): The catalog name (e.g., 'dbacademy_peter').
        in_schema (str): The schema name (e.g., 'default').
        dry_run (bool): If True, only prints tables that would be dropped without actually dropping them.

    Returns:
        list: Fully qualified names of the tables that were dropped (or would be dropped in dry-run mode).

    Example:
    >>> drop_tables(in_catalog='dbacademy_peter', in_schema='1_bronze_db')
    """
    # Check if catalog exists
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    if in_catalog not in catalogs:
        raise ValueError(f"Catalog '{in_catalog}' does not exist.")

    ## Delete tables and views in schema
    for schema in in_schema:
        
        # Check if schema exists in the catalog
        full_schema = f"{in_catalog}.{schema}"
        if not spark.catalog.databaseExists(full_schema):
            raise ValueError(f"Schema '{schema}' does not exist in catalog '{in_catalog}'.")
        
        print(f"\n{'Previewing' if dry_run else 'Dropping'} all tables in {in_catalog}.{schema}:")

        # Get all tables in the schema
        tables = spark.sql(f"SHOW TABLES IN {in_catalog}.{schema}").collect()

        if not tables:
            print(f"No tables found in schema {in_catalog}.{schema}. Nothing to drop.")
        else:
            table_names = [f"{in_catalog}.{schema}.{t.tableName}" for t in tables]

            for table_full_name in table_names:
                if dry_run:
                    print(f"Would drop: {table_full_name}")
                else:
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS {table_full_name}")
                        print(f"Dropped TABLE: {table_full_name}")
                    except:
                        spark.sql(f"DROP VIEW IF EXISTS {table_full_name}")
                        print(f"Dropped VIEW: {table_full_name}")

# COMMAND ----------

# DBTITLE 1,Display Configs
def display_config_values(config_values, copy_values=False):
    """
    Displays list of key-value pairs as rows of HTML text.

    Parameters
    ----------
    config_values : list of (key, value[, copy_value]) tuples
        key: text to display in "information" column
        value: text to display in "value" column (HTML safe; can include links)
        copy_value: per-row override for copy button. Can be specified as True or as a string value
                    if you want to copy something different than what's displayed. For example,
                    value might display a folder name while copy_value includes the entire path
    copy_values : bool, optional
        If True, a copy button appears next to each value. Default is False.

    Returns
    ----------
    HTML output displaying the config values

    Example
    --------
    display_config_values([('catalog', 'your catalog'), ('schema', 'your schema')])
    display_config_values([('catalog', 'your catalog')], copy_values=True)
    """
    rows = ""
    for name, value, *rest in config_values:
        copy_value = rest[0] if rest else (copy_values or "")
        copy_btn = ""
        if copy_value:
            if isinstance(copy_value, str):
                copy_value = f'data-copy="{copy_value}"'
            else:
                copy_value = ""
                
            copy_btn = """
                <button type="button" onclick="copyFromSibling(this)"
                    style="margin-left:auto;padding:2px 10px;border:1px solid #ccc;border-radius:4px;background:#f5f5f5;cursor:pointer;font-size:13px;flex:0 0 auto;min-width:68px; text-align:center; box-sizing:border-box;">
                    Copy
                </button>"""

        rows += f"""
        <tr>
          <td style="padding:6px 12px;white-space:nowrap;border-bottom:1px solid #e0e0e0;font-weight:600">{name}:</td>
          <td style="padding:6px 12px;border-bottom:1px solid #e0e0e0">
            <div style="display:flex; align-items:center; gap:12px;">
                <span class="copy-source" {copy_value} style="display:inline-block;padding:4px 8px;font-size:15px;min-width:0;">{value}</span>
                {copy_btn}
            </div>
          </td>
        </tr>"""

    html = """
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:#1b3a4b;color:#fff;padding:10px 16px;font-size:16px;font-weight:600">Configuration Values</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Information</th>
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Value</th>
        </tr>""" + rows + """
      </table>
    </div>
    <script>
        if (!window.copyFromSibling) {
            window.copyFromSibling = function(btn) {
            var source = btn.parentElement.querySelector('.copy-source');
            if (!source) return;

            var text = source.dataset.copy || source.innerText;

            var t = document.createElement('textarea');
            t.value = text;
            t.style.position = 'fixed';
            t.style.opacity = '0';
            t.style.pointerEvents = 'none';
            document.body.appendChild(t);
            t.select();
            document.execCommand('copy');
            document.body.removeChild(t);

            var original = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(function() {
                btn.textContent = original;
            }, 1500);
            };
        }
    </script>"""
    displayHTML(html)

# COMMAND ----------

# import os
# from databricks.sdk.service import jobs, pipelines
# from databricks.sdk import WorkspaceClient  

# class JobConfig:
#     '''
#     Example
#     ------------
#     job_tasks = [
#         {
#             'task_name': 'create_table',
#             'file_path': '/01 - Simple DAB/create_table',
#             'depends_on': None
#         },
#         {
#             'task_name': 'create_table1',
#             'file_path': '/01 - Simple DAB/other_table2',
#             'depends_on': [{'task_key': 'create_table'}]
#         },
#         {
#             'task_name': 'create_table3',
#             'file_path': '/01 - Simple DAB/other_table2',
#             'depends_on': [{'task_key': 'create_table'},{'task_key': 'create_table1'}]
#         }
#     ]


#     myjob = JobConfig(job_name='test3',
#                         job_tasks=job_tasks,
#                         job_parameters=[
#                             {'name':'target', 'default':'dev'},
#                             {'name':'catalog_name', 'default':'test'}
#                         ])
#     '''
#     def __init__(self, 
#                  job_name: str,
#                  job_tasks: list[dict],
#                  job_parameters: list[dict]):
    
#         self.job_name = job_name
#         self.job_tasks = job_tasks
#         self.job_parameters = job_parameters
        
#         ## Connect the Workspace
#         self.w = self.get_workspace_client()

#         ## Execute methods
#         self.check_for_duplicate_job_name(check_job_name=self.job_name)
#         print(f'Job name is unique. Creating the job {self.job_name}...')

#         self.course_path = self.get_path_one_folder_back()
#         self.list_job_tasks = self.create_job_tasks()

#         self.create_job(job_tasks = self.list_job_tasks)


#     ## Get Workspace client
#     def get_workspace_client(self):
#         """
#         Establishes and returns a WorkspaceClient instance for interacting with the Databricks API.
#         This is set when the object is created within self.w

#         Returns:
#             WorkspaceClient: A client instance to interact with the Databricks workspace.
#         """
#         w = WorkspaceClient()
#         return w


#     # Check if the job name already exists, return error if it does.
#     def check_for_duplicate_job_name(self, check_job_name: str):
#         for job in self.w.jobs.list():
#             if job.settings.name == check_job_name:
#                 test_job_name = False
#                 assert test_job_name, f'You already have a job with the same name. Please manually delete the job {self.job_name}'                


#     ## Store the path of one folder one folder back
#     def get_path_one_folder_back(self):
#         current_path = os.getcwd()
#         print(f'Using the following path to reference the Files: {current_path}/.')
#         return current_path


#     ## Create the job tasks
#     def create_job_tasks(self):
#         all_job_tasks = []
#         for task in job_tasks:
#             if task.get('file_path', False) != False:

#                 ## Create a list of jobs.TaskDependencies
#                 task_dependencies = [jobs.TaskDependency(task_key=depend_task['task_key']) for depend_task in task['depends_on']] if task['depends_on'] else None

#                 ## Create the task
#                 job_task_File = jobs.Task(task_key=task['task_name'],
#                                          notebook_task=jobs.NotebookTask(notebook_path=self.course_path+task['file_path']),
#                                          depends_on=task_dependencies,
#                                          timeout_seconds=0)
#                 all_job_tasks.append(job_task_File)

#             elif task.get('pipeline_task', False) != False:
#                 job_task_dlt = jobs.Task(task_key=task['task_name'],
#                                          pipeline_task=jobs.PipelineTask(pipeline_id=task['pipeline_id'], full_refresh=True),
#                                          timeout_seconds=0)
#                 all_job_tasks.append(job_task_info)

#         return all_job_tasks
    

#     def set_job_parameters(self, parameters: dict):

#         job_params_list = []
#         for param in self.job_parameters:
#             job_parameter = jobs.JobParameterDefinition(name=param['name'], default=param['default'])
#             job_params_list.append(job_parameter)

#         return job_params_list
    

#     ## Create final job
#     def create_job(self, job_tasks: list[jobs.Task]):
#         created_job = self.w.jobs.create(
#                 name=self.job_name,
#                 tasks=job_tasks,
#                 parameters = self.set_job_parameters(self.job_parameters)
#             )

# COMMAND ----------

import os
from databricks.sdk.service import jobs, pipelines
from databricks.sdk import WorkspaceClient


## Get Workspace client
def get_workspace_client():
    """
    Establishes and returns a WorkspaceClient instance for interacting
    with the Databricks API.
    """
    w = WorkspaceClient()
    return w


## Check if the job name already exists, return an error if it does.
def check_for_duplicate_job_name(w, check_job_name: str):
    for job in w.jobs.list():
        if job.settings.name == check_job_name:
            raise ValueError(
                f"You already have a job with the same name. "
                f"Please manually delete the job: {check_job_name}"
            )


## Store the current working path
def get_path_one_folder_back():
    current_path = os.getcwd()
    print(f"Using the following path to reference the Files: {current_path}/.")
    return current_path


## Create the job tasks
def create_job_tasks(job_tasks, course_path):
    all_job_tasks = []

    for task in job_tasks:

        ## Create a notebook task
        if task.get("file_path", False) != False:

            ## Create a list of jobs.TaskDependency objects
            task_dependencies = (
                [
                    jobs.TaskDependency(
                        task_key=depend_task["task_key"]
                    )
                    for depend_task in task["depends_on"]
                ]
                if task.get("depends_on")
                else None
            )

            ## Create the task
            job_task_file = jobs.Task(
                task_key=task["task_name"],
                notebook_task=jobs.NotebookTask(
                    notebook_path=course_path + task["file_path"]
                ),
                depends_on=task_dependencies,
                timeout_seconds=0
            )

            all_job_tasks.append(job_task_file)

        ## Create a pipeline / DLT task
        elif task.get("pipeline_task", False) != False:

            task_dependencies = (
                [
                    jobs.TaskDependency(
                        task_key=depend_task["task_key"]
                    )
                    for depend_task in task["depends_on"]
                ]
                if task.get("depends_on")
                else None
            )

            job_task_dlt = jobs.Task(
                task_key=task["task_name"],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=task["pipeline_id"],
                    full_refresh=True
                ),
                depends_on=task_dependencies,
                timeout_seconds=0
            )

            all_job_tasks.append(job_task_dlt)

    return all_job_tasks


## Create job parameters
def set_job_parameters(job_parameters):
    job_params_list = []

    for param in job_parameters:
        job_parameter = jobs.JobParameterDefinition(
            name=param["name"],
            default=param["default"]
        )

        job_params_list.append(job_parameter)

    return job_params_list


## Create final job
def create_job(w, job_name, job_tasks, job_parameters):
    created_job = w.jobs.create(
        name=job_name,
        tasks=job_tasks,
        parameters=set_job_parameters(job_parameters)
    )

    return created_job


## Main function: replaces the old JobConfig class
def create_job_config(
    job_name: str,
    job_tasks: list[dict],
    job_parameters: list[dict]
):
    ## Connect to the Workspace
    w = get_workspace_client()

    ## Check for a duplicate name
    check_for_duplicate_job_name(
        w=w,
        check_job_name=job_name
    )

    print(f"Job name is unique. Creating the job {job_name}...")

    ## Get the folder path
    course_path = get_path_one_folder_back()

    ## Convert your task dictionaries into Databricks task objects
    list_job_tasks = create_job_tasks(
        job_tasks=job_tasks,
        course_path=course_path
    )

    ## Create and return the Databricks job
    created_job = create_job(
        w=w,
        job_name=job_name,
        job_tasks=list_job_tasks,
        job_parameters=job_parameters
    )

    return created_job

# COMMAND ----------

def print_job_config(
    catalog_name,
    job_name_extension,
    file_paths,
    Files,
    job_tasks=None,
    check_task_dependencies=False
):
    """
    - Prints an HTML output in the cell below with information about the Job Name
      and Files to use with the path for each user.

    Parameters:
        - catalog_name (str): Schema name used in the required job name.
        - job_name_extension (str): Job name extension
            (job_name_extension_catalog_name).
        - file_paths (str): Folder location where the task files exist.
            Example: /Task Files/Lesson 1 Files
        - Files (list[str]): List of file names in the file_paths folder.
            Example: ['01-reset', '02-ingest']
        - job_tasks (dict): Required task names and their dependencies.
        - check_task_dependencies (bool): Determines whether to check
            dependencies in job tasks.

    Returns:
        - required_job_name
        - user_required_job_Files
        - job_tasks
        - check_task_dependencies
    """

    ## Get current path of File
    base_path = (
        dbutils.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .getOrElse(None)
    )

    ## Concatenates the schema name with the job name
    required_job_name = f"{job_name_extension}_{catalog_name}"

    ## Goes back to the main course folder
    main_course_folder_path = (
        "/Workspace"
        + "/".join(base_path.split("/")[:-1])
        + file_paths
        + "/"
    )

    ## Create paths for each File specified in method argument Files
    Files_to_print = []

    for i, File in enumerate(Files):
        current_file_path = (
            f"File #{i + 1}",
            main_course_folder_path + File
        )
        Files_to_print.append(current_file_path)

    ## Display Job Name and File paths as HTML output
    display_config_values(
        [("Job Name", required_job_name)] + Files_to_print
    )

    # ## Return values that were previously stored in the object
    # user_required_job_Files = [item[1] for item in Files_to_print]

    return 

# COMMAND ----------

# DBTITLE 1,Catalog
my_catalog = build_user_catalog(catalog_forced= None)
spark.sql(f'USE CATALOG {my_catalog}')

spark.sql(f'DECLARE OR REPLACE VARIABLE my_catalog STRING')
spark.sql(f"SET VAR my_catalog = '{my_catalog}'")

# COMMAND ----------

# DBTITLE 1,Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS jobs;
# MAGIC USE SCHEMA jobs;

# COMMAND ----------

create_volume(in_catalog = my_catalog,
              in_schema = 'jobs',
              volume_name = 'trigger_storage_location')

user_vol_path = f'/Volumes/{my_catalog}/jobs/'