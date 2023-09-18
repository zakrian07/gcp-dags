#!/usr/bin/env python
import os
import yaml
import time
import requests
from airflow.models import DagRun, TaskInstance
import pendulum
from pendulum import datetime, timezone
from datetime import timedelta
import logging
from jinja2 import Environment, BaseLoader
import json
from airflow.utils.state import TaskInstanceState
from airflow import DAG, XComArg
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.datafusion import \
  CloudDataFusionStartPipelineOperator
from airflow.utils.trigger_rule import TriggerRule

from gplan.gplan_ssa_outbound_utility import OutboundDagName, \
  replace_space_with_underscore, REGISTERED_SCHEDULAR
from gplan.operator import GoogleCloudStorageFileReaderOperator

CDF_INSTANCE_LOCATION = "us-central1"
CDF_INSTANCE_NAME = "info_here"

GPLAN_SSA_OUTBOUND_CDF_PIPELINE_TIMEOUT = int(
    os.getenv("GPLAN_SSA_OUTBOUND_CDF_PIPELINE_TIMEOUT", 54000))
GPLAN_SSA_OUTBOUND_CONFIG_BUCKET = os.getenv("GPLAN_SSA_OUTBOUND_CONFIG_BUCKET",
                                                       "info_here")
GPLAN_SSA_OUTBOUND_CONFIG_PATH = os.getenv("GPLAN_SSA_OUTBOUND_CONFIG_PATH",
                                                     "info_here.yaml")
GPLAN_BATCH_COMMIT_ENDPOINT = os.getenv("GPLAN_BATCH_COMMIT_ENDPOINT",
                                        "info_here/v1/planning/batchcommits")
GPLAN_OAUTH_URL = os.getenv("GPLAN_OAUTH_URL","info_here.googleapis.com/token")
GPLAN_CLIENT_ID = os.getenv("GPLAN_CLIENT_ID","info_here")
GPLAN_CLIENT_SECRET = os.getenv("GPLAN_CLIENT_SECRET","info_here")
GPLAN_REFRESH_TOKEN = os.getenv("GPLAN_REFRESH_TOKEN","info_here")
GPLAN_BATCH_INSERT_ENDPOINT = os.getenv("GPLAN_BATCH_INSERT_ENDPOINT",
                                        "https://autopush-tisupplychain.sandbox.googleapis.com/v1/planning/batchreceipt:batchCreate")
GPLAN_TOKEN_ENDPOINT = os.getenv("GPLAN_TOKEN_ENDPOINT","info_here:generateAccessToken")

CURRENT_TIMESTAMP=pendulum.now()

default_arguments = {
    "owner": "omni-gplan"
}
dag_definition = {
    "dag_id": OutboundDagName.PRIMARY,
    "description": "This is the main dag for orchestrating the outbound worksheet retrieval flow."
                   "It will be triggered by the scheduled dags or manually."
                   "Based on the entity who triggered this dag, it will construct the payload to"
                   "send to CDF pipeline and pull worksheet data from rapid response and dump into"
                   "google cloud storage bucket",
    "start_date": datetime(2023, 3, 30, tz=timezone("America/Los_Angeles")),
    "default_args": default_arguments,
    "default_view": "graph",
    "tags": ["CDF_TRIGGER", "PRIMARY", "SSA_OUTBOUND"],
    "params": {
        "triggered_by": Param(
            default="",
            description="upstream system/pipeline that is triggering this dag"
        ),
        "payload": Param(
            default={}
        )
    },
    "render_template_as_native_obj": True,
    "schedule": None,
    "catchup": False
}

logger = logging.getLogger("airflow.task")


def inspect_dag_triggered_by(**context) -> str:
  params = context.get("params")
  triggered_by = params.get("triggered_by")

  if triggered_by in REGISTERED_SCHEDULAR:
    return "cc_task_render_metadata_scheduled_trigger"

  return "cc_task_render_metadata_manual_trigger"


def verify_input_configuration(**context) -> str:
  params = context.get("params")
  triggered_by = params.get("triggered_by")
  payload = params.get("payload")

  # in case the dag was triggered manually and user did not pass 'triggered_by' param
  if not triggered_by:
    return "cc_task_exit_on_invalid_payload"

  # when dag is triggered manually, it must provide payload. If not
  # we should bypass all the steps and exit the dag (short circuit)
  # TODO: Add json schema validation to payload such that it must have worksheet, scenario, workbook and source_name
  if (triggered_by not in REGISTERED_SCHEDULAR) and (not payload):
    return "cc_task_exit_on_invalid_payload"

  return "cc_task_load_external_metadata_configuration"


def build_cdf_pipeline_payload(**context) -> list:
  task_instance: TaskInstance = context.get("task_instance")
  triggered_by = task_instance.xcom_pull("cc_task_inspect_dag_triggered_by")

  return task_instance.xcom_pull(triggered_by)


def render_metadata_for_manual_trigger(configuration: str, **context):
  logger.info("rendering metadata for scheduled trigger")
  dag_run: DagRun = context.get("dag_run")
  payload = context.get("params").get("payload")
  print("PAYLOAD",payload)
  dag_logical_date = dag_run.logical_date.strftime("%Y-%m-%d")
  current_date = dag_run.logical_date.strftime("%Y%m%d")
  previous_date = (dag_run.logical_date - timedelta(days=1)).strftime("%Y%m%d")
  dag_run_id = dag_run.run_id

  configuration_dict = yaml.safe_load(configuration)

  rapid_response_entities = configuration_dict.get("rapid_response_entities")
  source_rapid_response_instances = configuration_dict.get(
      "source_rapid_response_instances")
  available_destinations = configuration_dict.get("transformed")
  available_sources = configuration_dict.get("raw")

  registered_rapid_response_entities = payload

  xcom_payload = []

  for entity_name in registered_rapid_response_entities:

    identified_rapid_response_entity = list(
        filter(
            lambda item: item.get("name") == entity_name,
            rapid_response_entities
        )
    )
    if len(identified_rapid_response_entity) == 0:
      logger.info(
          f"no rapid response entity found in configuration with name: {entity_name}")
      continue

    identified_rapid_response_entity = identified_rapid_response_entity[0]

    if not identified_rapid_response_entity.get("active"):
      logger.info(
          f"identified rapid response entity {identified_rapid_response_entity.get('name')} is not active")
      continue

    rr_source = identified_rapid_response_entity.get("rr_source")
    workbook = identified_rapid_response_entity.get("workbook")
    worksheet = identified_rapid_response_entity.get("worksheet")
    scenario = identified_rapid_response_entity.get("scenario")
    batch_create_body = identified_rapid_response_entity.get("batch_create_body")
    pageSize = identified_rapid_response_entity.get("pageSize")
    recordEntity = identified_rapid_response_entity.get("recordEntity")
    sinkBatchSize = identified_rapid_response_entity.get("sinkBatchSize")
    schema = identified_rapid_response_entity.get("worksheetSchema")
    cdf_pipeline_name = identified_rapid_response_entity.get("pipeline")

    entity_destination = list(
        filter(
            lambda item: item.get("name") == identified_rapid_response_entity.get(
                "transformed").get("name"),
            available_destinations
        )
    )
    if len(entity_destination) == 0:
      logger.info(
          f"destination {identified_rapid_response_entity.get('transformed').get('name')} is"
          f"not a valid destination")
      continue

    entity_destination = entity_destination[0]

    rendered_file_path_dest = str()
    rendered_file_prefix_dest = str()

    env = Environment(loader=BaseLoader())
    if entity_destination.get("type") == "GCS":
      gcs_bucket = entity_destination.get("bucket")
      base_path = entity_destination.get("base_path")

      rendered_base_path = env.from_string(base_path).render(dict(
          rr_source=replace_space_with_underscore(rr_source),
          scenario=replace_space_with_underscore(scenario),
          workbook=replace_space_with_underscore(workbook),
          worksheet=replace_space_with_underscore(worksheet),
          dag_run_id=dag_run_id,
          pipeline_name=cdf_pipeline_name,
          dag_logical_date=dag_logical_date)
      )
      rendered_file_path_dest = f"gs://{gcs_bucket}/{rendered_base_path}"
      rendered_file_prefix_dest = identified_rapid_response_entity.get(
          "transformed").get("file_prefix")
    else:
      # TODO: Handling of xsal destination will be added once its designed
      logger.info(
          f"destination of type '{entity_destination.get('type')}' is not yet implemented")
      continue

    rendered_file_path_source = str()
    rendered_file_prefix_source = str()

    entity_source = list(
        filter(
            lambda item: item.get(
                "name") == identified_rapid_response_entity.get("raw").get(
                "name"),
            available_sources
        )
    )
    if len(entity_source) == 0:
      logger.info(
          f"source {identified_rapid_response_entity.get('raw').get('name')} is"
          f"not valid.")
      continue

    entity_source = entity_source[0]

    env = Environment(loader=BaseLoader())
    if entity_source.get("type") == "GCS":
      gcs_bucket = entity_source.get("bucket")
      base_path = entity_source.get("base_path")

      rendered_base_path = env.from_string(base_path).render(dict(
          rr_source=replace_space_with_underscore(rr_source),
          scenario=replace_space_with_underscore(scenario),
          workbook=replace_space_with_underscore(workbook),
          worksheet=replace_space_with_underscore(worksheet),
          dag_run_id=dag_run_id,
          pipeline_name=cdf_pipeline_name,
          dag_logical_date=dag_logical_date)
      )
      rendered_file_path_source = f"gs://{gcs_bucket}/{rendered_base_path}"
      rendered_file_prefix_source = identified_rapid_response_entity.get(
          "raw").get(
          "file_prefix")
    else:
      # TODO: Handling of xsal destination will be added once its designed
      logger.info(
          f"source of type '{entity_source.get('type')}' is not yet implemented")
      continue

    identified_source_rapid_response_instance = list(
        filter(
            lambda item: item.get("name") == rr_source,
            source_rapid_response_instances
        )
    )

    if len(identified_source_rapid_response_instance) == 0:
      logger.info(
          f"rapid response instance {rr_source} not found in metadata configuration, skipping {worksheet}"
      )
      continue

    identified_source_rapid_response_instance = \
      identified_source_rapid_response_instance[0]

    rapid_response_fqdn = identified_source_rapid_response_instance.get("fqdn")
    rapid_response_env = identified_source_rapid_response_instance.get(
        "instance")
    rapid_response_base_url = f"https://{rapid_response_fqdn}/{rapid_response_env}"
    client_id_key = identified_source_rapid_response_instance.get("oauth").get(
        "client_id").get("name")
    client_secret_key = identified_source_rapid_response_instance.get(
        "oauth").get("client_secret").get("name")
    oauth_client_id = "${secure(%s)}" % client_id_key
    oauth_client_secret = "${secure(%s)}" % client_secret_key
    rendered_schema = env.from_string(schema).render(
        dict(
            current_date=current_date,
            previous_date=previous_date
        )
    )
    rendered_schema = json.loads(rendered_schema)
    rendered_schema["gcsDestinationPath"] = rendered_file_path_dest
    rendered_schema["gcsDestinationFilePrefix"] = rendered_file_prefix_dest
    rendered_schema["gcsSourcePath"] = rendered_file_path_source
    rendered_schema["gcsSourceFilePrefix"] = rendered_file_prefix_source
    rendered_schema["oauthClientId"] = oauth_client_id
    rendered_schema["oauthClientSecret"] = oauth_client_secret
    rendered_schema["rapidResponseBaseUrl"] = rapid_response_base_url

    payload = {
        "pipeline_name": cdf_pipeline_name,
        "runtime_args": {
            "worksheetSchema": schema,
            "gcsDestinationPath":rendered_file_path_dest,
            "gcsDestinationFilePrefix":rendered_file_prefix_dest,
            "gcsSourcePath":rendered_file_path_source,
            "gcsSourceFilePrefix":rendered_file_prefix_source,
            "oauthClientId":oauth_client_id,
            "oauthClientSecret":oauth_client_secret,
            "rapidResponseBaseUrl":rapid_response_base_url,
            "batch_create_body":batch_create_body,
            "recordEntity":recordEntity,
            "sinkBatchSize":sinkBatchSize,
            "pageSize":pageSize,
            "sourceSystem":rr_source,
            "batchInsertSink":GPLAN_BATCH_INSERT_ENDPOINT
        }

    }
    xcom_payload.append(payload)
  return xcom_payload


def render_metadata_for_scheduled_trigger(configuration: str, **context):
  logger.info("rendering metadata for scheduled trigger")
  dag_run: DagRun = context.get("dag_run")
  triggered_by = context.get("params").get("triggered_by")

  dag_logical_date = dag_run.logical_date.strftime("%Y-%m-%d")
  current_date = dag_run.logical_date.strftime("%Y%m%d")
  previous_date = (dag_run.logical_date - timedelta(days=1)).strftime("%Y%m%d")
  dag_run_id = dag_run.run_id

  configuration_dict = yaml.safe_load(configuration)

  rapid_response_entities = configuration_dict.get("rapid_response_entities")
  source_rapid_response_instances = configuration_dict.get(
      "source_rapid_response_instances")
  available_destinations = configuration_dict.get("transformed")
  available_sources = configuration_dict.get("raw")
  available_schedules = configuration_dict.get("schedules")

  identified_schedule = list(
      filter(
          lambda item: item.get("dag_id") == triggered_by,
          available_schedules
      )
  )

  if len(identified_schedule) == 0:
    logger.info(
        f"schedular dag {triggered_by} not found in configuration, get this schedule registered")
    return []

  identified_schedule = identified_schedule[0]
  registered_rapid_response_entities = identified_schedule.get(
      "target_rr_entities")

  xcom_payload = []

  for entity_name in registered_rapid_response_entities:

    identified_rapid_response_entity = list(
        filter(
            lambda item: item.get("name") == entity_name,
            rapid_response_entities
        )
    )
    if len(identified_rapid_response_entity) == 0:
      logger.info(
          f"no rapid response entity found in configuration with name: {entity_name}")
      continue

    identified_rapid_response_entity = identified_rapid_response_entity[0]

    if not identified_rapid_response_entity.get("active"):
      logger.info(
          f"identified rapid response entity {identified_rapid_response_entity.get('name')} is not active")
      continue

    rr_source = identified_rapid_response_entity.get("rr_source")
    workbook = identified_rapid_response_entity.get("workbook")
    worksheet = identified_rapid_response_entity.get("worksheet")
    scenario = identified_rapid_response_entity.get("scenario")
    batch_create_body = identified_rapid_response_entity.get("batch_create_body")
    pageSize = identified_rapid_response_entity.get("pageSize")
    recordEntity = identified_rapid_response_entity.get("recordEntity")
    sinkBatchSize = identified_rapid_response_entity.get("sinkBatchSize")
    schema = identified_rapid_response_entity.get("worksheetSchema")
    cdf_pipeline_name = identified_rapid_response_entity.get("pipeline")

    entity_destination = list(
        filter(
            lambda item: item.get("name") == identified_rapid_response_entity.get(
                "transformed").get("name"),
            available_destinations
        )
    )
    if len(entity_destination) == 0:
      logger.info(
          f"destination {identified_rapid_response_entity.get('transformed').get('name')} is"
          f"not a valid destination")
      continue

    entity_destination = entity_destination[0]

    rendered_file_path_dest = str()
    rendered_file_prefix_dest = str()

    env = Environment(loader=BaseLoader())
    if entity_destination.get("type") == "GCS":
      gcs_bucket = entity_destination.get("bucket")
      base_path = entity_destination.get("base_path")

      rendered_base_path = env.from_string(base_path).render(dict(
          rr_source=replace_space_with_underscore(rr_source),
          scenario=replace_space_with_underscore(scenario),
          workbook=replace_space_with_underscore(workbook),
          worksheet=replace_space_with_underscore(worksheet),
          dag_run_id=dag_run_id,
          pipeline_name=cdf_pipeline_name,
          dag_logical_date=dag_logical_date)
      )
      rendered_file_path_dest = f"gs://{gcs_bucket}/{rendered_base_path}"
      rendered_file_prefix_dest = identified_rapid_response_entity.get(
          "transformed").get("file_prefix")
    else:
      # TODO: Handling of xsal destination will be added once its designed
      logger.info(
          f"destination of type '{entity_destination.get('type')}' is not yet implemented")
      continue

    rendered_file_path_source = str()
    rendered_file_prefix_source = str()

    entity_source = list(
        filter(
            lambda item: item.get(
                "name") == identified_rapid_response_entity.get("raw").get(
                "name"),
            available_sources
        )
    )
    if len(entity_source) == 0:
      logger.info(
          f"source {identified_rapid_response_entity.get('raw').get('name')} is"
          f"not valid.")
      continue

    entity_source = entity_source[0]

    env = Environment(loader=BaseLoader())
    if entity_source.get("type") == "GCS":
      gcs_bucket = entity_source.get("bucket")
      base_path = entity_source.get("base_path")

      rendered_base_path = env.from_string(base_path).render(dict(
          rr_source=replace_space_with_underscore(rr_source),
          scenario=replace_space_with_underscore(scenario),
          workbook=replace_space_with_underscore(workbook),
          worksheet=replace_space_with_underscore(worksheet),
          dag_run_id=dag_run_id,
          pipeline_name=cdf_pipeline_name,
          dag_logical_date=dag_logical_date)
      )
      rendered_file_path_source = f"gs://{gcs_bucket}/{rendered_base_path}"
      rendered_file_prefix_source = identified_rapid_response_entity.get(
          "raw").get(
          "file_prefix")
    else:
      # TODO: Handling of xsal destination will be added once its designed
      logger.info(
          f"source of type '{entity_source.get('type')}' is not yet implemented")
      continue

    identified_source_rapid_response_instance = list(
        filter(
            lambda item: item.get("name") == rr_source,
            source_rapid_response_instances
        )
    )

    if len(identified_source_rapid_response_instance) == 0:
      logger.info(
          f"rapid response instance {rr_source} not found in metadata configuration, skipping {worksheet}"
      )
      continue

    identified_source_rapid_response_instance = \
      identified_source_rapid_response_instance[0]

    rapid_response_fqdn = identified_source_rapid_response_instance.get("fqdn")
    rapid_response_env = identified_source_rapid_response_instance.get(
        "instance")
    rapid_response_base_url = f"https://{rapid_response_fqdn}/{rapid_response_env}"
    client_id_key = identified_source_rapid_response_instance.get("oauth").get(
        "client_id").get("name")
    client_secret_key = identified_source_rapid_response_instance.get(
        "oauth").get("client_secret").get("name")
    oauth_client_id = "${secure(%s)}" % client_id_key
    oauth_client_secret = "${secure(%s)}" % client_secret_key
    rendered_schema = env.from_string(schema).render(
        dict(
            current_date=current_date,
            previous_date=previous_date
        )
    )
    rendered_schema = json.loads(rendered_schema)
    rendered_schema["gcsDestinationPath"] = rendered_file_path_dest
    rendered_schema["gcsDestinationFilePrefix"] = rendered_file_prefix_dest
    rendered_schema["gcsSourcePath"] = rendered_file_path_source
    rendered_schema["gcsSourceFilePrefix"] = rendered_file_prefix_source
    rendered_schema["oauthClientId"] = oauth_client_id
    rendered_schema["oauthClientSecret"] = oauth_client_secret
    rendered_schema["rapidResponseBaseUrl"] = rapid_response_base_url

    payload = {
        "pipeline_name": cdf_pipeline_name,
        "runtime_args": {
            "worksheetSchema": schema,
            "gcsDestinationPath":rendered_file_path_dest,
            "gcsDestinationFilePrefix":rendered_file_prefix_dest,
            "gcsSourcePath":rendered_file_path_source,
            "gcsSourceFilePrefix":rendered_file_prefix_source,
            "oauthClientId":oauth_client_id,
            "oauthClientSecret":oauth_client_secret,
            "rapidResponseBaseUrl":rapid_response_base_url,
            "batch_create_body":batch_create_body,
            "recordEntity":recordEntity,
            "sinkBatchSize":sinkBatchSize,
            "pageSize":pageSize,
            "sourceSystem":rr_source,
            "batchInsertSink":GPLAN_BATCH_INSERT_ENDPOINT
        }

    }
    xcom_payload.append(payload)
  return xcom_payload

def get_token() -> str:
    try:
        auth = (GPLAN_CLIENT_ID, GPLAN_CLIENT_SECRET)
        params = {"grant_type": "refresh_token", "refresh_token": GPLAN_REFRESH_TOKEN}
        token_response = requests.post(GPLAN_OAUTH_URL, auth=auth, data=params)
        
        # Raise an exception if the request was unsuccessful
        token_response.raise_for_status()
        
        token = token_response.json()["access_token"]
        return token
    except requests.RequestException as e:
        logger.error(f"Failed to get token: {e}")
        raise
    except KeyError:
        logger.error("Key 'access_token' not found in the response")
        raise

def post_trigger(runtime_args):
  """Function to trigger POST API call."""
  post_body=[]
  final_runtime_args=[]
  for i in runtime_args:
    print(i)
    runtime_args=i.get("runtime_args")
    runtime_args["batch_create_body"]=runtime_args["batch_create_body"].replace("{{name}}",'"'+str(json.loads(runtime_args["worksheetSchema"])["WorkbookParameters"]["WorksheetNames"][0])+"_"+str(CURRENT_TIMESTAMP)+'"')
    batch_create_body = runtime_args.get("batch_create_body")
    print(type(batch_create_body))
    print(batch_create_body)
    endpoint = GPLAN_BATCH_COMMIT_ENDPOINT
    response = requests.post(
        endpoint,
        data=str(batch_create_body),
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + get_token()},
    )
    response=response.json()
    print("RESPONSE: ",response)

    runtime_args["sourceSnapshotTime"]=str(CURRENT_TIMESTAMP)
    runtime_args["batchId"]=str(response["batchId"])
    i["runtime_args"]=runtime_args
    final_runtime_args.append(i)
  return final_runtime_args


  print("pulled_messages ",pulled_messages)
  print("context ", context)
  completed = []
  failed = []
  dag_run = context.get("dag_run")
  params = context.get("params")

  for index in range(len(pulled_messages)):
    completed = []
    failed = []
    task_instance = dag_run.get_task_instance(task_id="cc_task_trigger_cdf_pipeline", map_index=index)
    logger.info(f"Task: {task_instance.task_id}-{task_instance.map_index} | State: {task_instance.state.upper()}")
    print("task_instance",task_instance)
    print("task_instance.state", task_instance.state)
    print("TaskInstanceState.SUCCESS",TaskInstanceState.SUCCESS)

    print("pulled_messages in patch",pulled_messages)
    pulled_messages_temp=[pulled_messages[index]]
    print(pulled_messages_temp)
    if task_instance.state == TaskInstanceState.SUCCESS:
      completed.append(pulled_messages_temp)
      for i in completed:
        for entry in i:
          batch_success_body = {"batchId":entry["runtime_args"]["batchId"],
                                "sourceSystem":entry["runtime_args"]["sourceSystem"],
                                "batchState":"COMPLETED"}
          print("batch_success_body ",batch_success_body)
          batch_name=str(json.loads(entry["runtime_args"]["batch_create_body"].replace("'",'''"'''))["name"])
          endpoint = GPLAN_BATCH_COMMIT_ENDPOINT+"/"+batch_name
          print("COMPLETED ENDPOINT", endpoint)
          response = requests.patch(
              endpoint,
              data=str(batch_success_body),
              headers={
                  'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + get_token()},
          )
          response=response.json()
          print("COMPLETED RESPONSE",response)
      print("COMPLETED ",completed)
    else:
      failed.append(pulled_messages_temp)
      for i in failed:
        for entry in i:
          print(type(entry))
          batch_fail_body = {"batchId":entry["runtime_args"]["batchId"],
                             "sourceSystem":entry["runtime_args"]["sourceSystem"],
                             "batchState":"FAILED"}
          print("batch_fail_body ",batch_fail_body)
          batch_name=str(json.loads(entry["runtime_args"]["batch_create_body"].replace("'",'''"'''))["name"])
          print(batch_name)
          endpoint = GPLAN_BATCH_COMMIT_ENDPOINT+"/"+batch_name
          print("FAILED ENDPOONT",endpoint)
          response = requests.patch(
              endpoint,
              data=str(batch_fail_body),
              headers={
                  'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + get_token()},
          )
          response=response.json()
          print("FAILED RESPONSE",response)
    print("FAILED ",failed)
def patch_trigger(pulled_messages, **context):
    try:
        logger.info(f"pulled_messages: {pulled_messages}, context: {context}")
        completed = []
        failed = []
        dag_run = context.get("dag_run")

        for index, message in enumerate(pulled_messages):
            task_instance = dag_run.get_task_instance(task_id="cc_task_trigger_cdf_pipeline", map_index=index)
            logger.info(f"Task: {task_instance.task_id}-{task_instance.map_index} | State: {task_instance.state.upper()}")

            if task_instance.state == TaskInstanceState.SUCCESS:
                completed.append(message)
                batch_success_body = {
                    "batchId": message["runtime_args"]["batchId"],
                    "sourceSystem": message["runtime_args"]["sourceSystem"],
                    "batchState": "COMPLETED"
                }
                batch_name = json.loads(message["runtime_args"]["batch_create_body"].replace("'", '"'))["name"]
                endpoint = f"{GPLAN_BATCH_COMMIT_ENDPOINT}/{batch_name}"

                response = requests.patch(
                    endpoint,
                    json=batch_success_body,
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {get_token()}'
                    }
                )
                
                # Raise an exception if the request was unsuccessful
                response.raise_for_status()

                logger.info(f"COMPLETED RESPONSE: {response.json()}")

            else:
                failed.append(message)
                batch_fail_body = {
                    "batchId": message["runtime_args"]["batchId"],
                    "sourceSystem": message["runtime_args"]["sourceSystem"],
                    "batchState": "FAILED"
                }
                batch_name = json.loads(message["runtime_args"]["batch_create_body"].replace("'", '"'))["name"]
                endpoint = f"{GPLAN_BATCH_COMMIT_ENDPOINT}/{batch_name}"

                response = requests.patch(
                    endpoint,
                    json=batch_fail_body,
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {get_token()}'
                    }
                )
                
                # Raise an exception if the request was unsuccessful
                response.raise_for_status()

                logger.info(f"FAILED RESPONSE: {response.json()}")

        logger.info(f"COMPLETED: {completed}, FAILED: {failed}")

    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except KeyError as e:
        logger.error(f"Key error: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

with DAG(**dag_definition) as dag:
    task_one = EmptyOperator(task_id="cc_task_starting")

    task_two = BranchPythonOperator(
        task_id="cc_task_verify_input_configuration",
        provide_context=True,
        python_callable=verify_input_configuration
    )

    task_three_branch_one = GoogleCloudStorageFileReaderOperator(
        task_id="cc_task_load_external_metadata_configuration",
        bucket_name=GPLAN_SSA_OUTBOUND_CONFIG_BUCKET,
        file_path=GPLAN_SSA_OUTBOUND_CONFIG_PATH,
    )

    task_three_branch_two = BashOperator(
        task_id="cc_task_exit_on_invalid_payload",
        bash_command="echo 'exiting dag run execution as provided payload is not valid'"
    )

    task_four_branch_one = BranchPythonOperator(
        task_id="cc_task_inspect_dag_triggered_by",
        provide_context=True,
        python_callable=inspect_dag_triggered_by
    )

    task_five_branch_one_alpha = PythonOperator(
        task_id="cc_task_render_metadata_manual_trigger",
        provide_context=True,
        python_callable=render_metadata_for_manual_trigger,
        op_kwargs={
            "configuration": "{{ task_instance.xcom_pull(task_ids='cc_task_load_external_metadata_configuration') }}"
        }
    )

    task_five_branch_one_beta = PythonOperator(
        task_id="cc_task_render_metadata_scheduled_trigger",
        provide_context=True,
        python_callable=render_metadata_for_scheduled_trigger,
        op_kwargs={
            "configuration": "{{ task_instance.xcom_pull(task_ids='cc_task_load_external_metadata_configuration') }}"
        }
    )

    task_six_branch_one = PythonOperator(
        task_id="cc_task_build_cdf_pipeline_payload",
        provide_context=True,
        python_callable=build_cdf_pipeline_payload,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    task_batch_start = PythonOperator(
        task_id='cc_task_create_batch_commit',
        python_callable=post_trigger,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        op_args=[task_six_branch_one.output],
    )

    task_seven_branch_one = CloudDataFusionStartPipelineOperator.partial(
        task_id="cc_task_trigger_cdf_pipeline",
        pipeline_timeout=GPLAN_SSA_OUTBOUND_CDF_PIPELINE_TIMEOUT,
        location=CDF_INSTANCE_LOCATION,
        instance_name=CDF_INSTANCE_NAME,
        success_states=["COMPLETED"],
        retries=0,
        retry_exponential_backoff=True
    ).expand_kwargs(task_batch_start.output)

    task_batch_end = PythonOperator(
        task_id='cc_task_update_batch_commit',
        python_callable=patch_trigger,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={
            "pulled_messages": "{{ task_instance.xcom_pull(task_ids='cc_task_create_batch_commit') }}"
        },
    )

    task_eight = EmptyOperator(task_id="cc_task_ending")

    # Task dependencies
    task_one >> task_two >> [task_three_branch_one, task_three_branch_two]
    task_three_branch_one >> task_four_branch_one >> [task_five_branch_one_alpha, task_five_branch_one_beta]
    task_five_branch_one_alpha >> task_six_branch_one
    task_five_branch_one_beta >> task_six_branch_one
    task_six_branch_one >> task_batch_start >> task_seven_branch_one >> task_batch_end >> task_eight

