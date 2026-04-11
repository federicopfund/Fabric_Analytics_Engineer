import logging
import os
import azure.functions as func

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService

def main(myblob: func.InputStream):

    job_id = os.environ.get("DB_JOB_ID")

    # Let's create Databricks CLI API client to be able to interact with Databricks REST API
    config = EnvironmentVariableConfigProvider().get_config()
    api_client = _get_api_client(config, command_name="cicdtemplates-")
    jobs_service = JobsService(api_client)

    # get a list of all active runs for the desired job
    active_runs = jobs_service.list_runs(job_id=job_id, active_only=True)

    logging.info(active_runs)

    if "runs" not in active_runs:
        logging.info(f"No active runs for job {job_id}. Triggering a new run!")
        jobs_service.run_now(job_id=job_id, notebook_params=None)
    elif  len(active_runs["runs"]) == 1:
        logging.info(f"Found an active run for job {job_id}. Skipping...")
    else:
        raise Exception(f"Job {job_id} has more than 1 active run. Please check your job configuration.")

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")