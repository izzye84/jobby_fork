import os
from typing import Tuple, List

from jobby.dbt_cloud import DBTCloud
from jobby.operations import generate_dot_graph
from jobby.schemas import Job, Model
from jobby.dag import DAG
from jobby.manifest import Manifest


from jobby.operations import distribute_job

api_key = os.environ.get("API_KEY")

if api_key is None:
    raise Exception("An API_KEY env var must be provided.")

dbt_cloud = DBTCloud(account_id=10767, api_key=api_key)
manifest = Manifest(manifest_path="example-data/manifest.json")


# Daily jobs
daily_stage_and_intermediate = dbt_cloud.get_job(job_id=43696, dag=manifest.dag)
daily_oc11_base_cloud = dbt_cloud.get_job(job_id=43020, dag=manifest.dag)
daily_eom_base_cloud = dbt_cloud.get_job(job_id=43018, dag=manifest.dag)

print(daily_stage_and_intermediate)

# Hourly jobs
hourly_stage_cloud = dbt_cloud.get_job(job_id=43030, dag=manifest.dag)
hourly_orders_intermediate_cloud = dbt_cloud.get_job(job_id=43029, dag=manifest.dag)
hourly_marts_cloud = dbt_cloud.get_job(job_id=38468, dag=manifest.dag)

hourly_countwise = dbt_cloud.get_job(job_id=20449, dag=manifest.dag)
stockmaster_timeline = dbt_cloud.get_job(job_id=27226, dag=manifest.dag)

customer_dist_order_fct = dbt_cloud.get_job(job_id=35322, dag=manifest.dag)
stage_hourly = dbt_cloud.get_job(job_id=38449, dag=manifest.dag)
demand_hour_day_segment_fill_loc = dbt_cloud.get_job(job_id=60163, dag=manifest.dag)
finance_sales_validated = dbt_cloud.get_job(job_id=98859, dag=manifest.dag)

labor_forcast_and_sales_plan = dbt_cloud.get_job(job_id=26613, dag=manifest.dag)

snapshots_hourly = dbt_cloud.get_job(job_id=38454, dag=manifest.dag)

hourly_jobs = [
    hourly_stage_cloud,
    hourly_orders_intermediate_cloud,
    hourly_marts_cloud,
    stockmaster_timeline,
    hourly_countwise,
    customer_dist_order_fct,
    stage_hourly,
    demand_hour_day_segment_fill_loc,
    finance_sales_validated,
    labor_forcast_and_sales_plan,
    snapshots_hourly,
]

dot_graph = generate_dot_graph(hourly_jobs, name="finish_line")
dot_graph.write_dot("finish_line_hourly.dot")  # type: ignore
dot_graph.write_pdf("finish_line_hourly.pdf")  # type: ignore


jobs = [
    daily_oc11_base_cloud,
    daily_eom_base_cloud,
    hourly_stage_cloud,
    hourly_orders_intermediate_cloud,
    hourly_marts_cloud,
    hourly_countwise,
    stockmaster_timeline,
    customer_dist_order_fct,
    stage_hourly,
    demand_hour_day_segment_fill_loc,
    finance_sales_validated,
    labor_forcast_and_sales_plan,
    daily_stage_and_intermediate,
    snapshots_hourly,
]


dot_graph = generate_dot_graph(jobs, name="finish_line")
dot_graph.write_dot("finish_line_current.dot")  # type: ignore
dot_graph.write_pdf("finish_line_current.pdf")  # type: ignore

# Distribute Base OEM and Base OC11 between daily and hourly staging jobs

updated_targets, eom_remainder = distribute_job(
    dag=manifest.dag,
    source_job=daily_eom_base_cloud,
    target_jobs=[hourly_stage_cloud, daily_stage_and_intermediate],
)

if eom_remainder is not None:
    raise Exception(
        f"The job {daily_eom_base_cloud.name} was not evenly divided. Remaining models: {eom_remainder.models}."
    )

updated_targets, oc11_remainder = distribute_job(
    dag=manifest.dag,
    source_job=daily_oc11_base_cloud,
    target_jobs=[item for key, item in updated_targets.items()],
)

if oc11_remainder is not None:
    raise Exception(
        f"The job {daily_oc11_base_cloud.name} was not evenly divided. Remaining models: {oc11_remainder.models}."
    )


jobs = [
    demand_hour_day_segment_fill_loc,
    finance_sales_validated,
    labor_forcast_and_sales_plan,
    updated_targets[daily_stage_and_intermediate.job_id],
    updated_targets[hourly_stage_cloud.job_id],
    hourly_orders_intermediate_cloud,
    hourly_marts_cloud,
    hourly_countwise,
    stockmaster_timeline,
    customer_dist_order_fct,
    stage_hourly,
    snapshots_hourly,
]

dot_graph = generate_dot_graph(jobs, name="finish_line")

dot_graph.write_dot("finish_line_distributed.dot")  # type: ignore
dot_graph.write_pdf("finish_line_distributed.pdf")  # type: ignore


# Union together the hourly jobs that have dependencies
jobs_to_union = [
    updated_targets[hourly_stage_cloud.job_id],
    hourly_orders_intermediate_cloud,
    hourly_marts_cloud,
    hourly_countwise,
    stockmaster_timeline,
    customer_dist_order_fct,
    stage_hourly,
    snapshots_hourly,
]


unioned_jobs = jobs_to_union[0].union(jobs_to_union[1:-1])
unioned_jobs.name = "Hourly Models"


jobs = [
    demand_hour_day_segment_fill_loc,
    finance_sales_validated,
    labor_forcast_and_sales_plan,
    updated_targets[daily_stage_and_intermediate.job_id],
    unioned_jobs,
]


dot_graph = generate_dot_graph(jobs, name="finish_line_unioned")
dot_graph.write_dot("finish_line_unioned.dot")  # type: ignore
dot_graph.write_pdf("finish_line_unioned.pdf")  # type: ignore

# Fine tuning to rebalance model dependencies
def transfer_models(model_names: List[str], source_job: Job, target_job: Job) -> None:
    """Move a model from one job and place it in another job."""
    for model_name in model_names:
        model: Model = source_job.pop_model(model_name=model_name)
        target_job.add_model(model)


# Move models from daily into hourly to minimize cross-job dependencies
transfer_models(
    model_names=[
        "stg_oc11_dcs_sku_current_lens",
        "stg_stock_master",
        "stg_eom_purchase_orders_attribute",
        "base_eom_purchase_orders_attribute",
        "stg_tfl_stock_master",
        "stg_countwise_chains",
        "stg_countwise_sites_in_chain",
        "stg_countwise_location",
        "stg_tfl_loc_partic",
        "stg_tfl_loc_master",
        "stg_tfl_district_master",
    ],
    source_job=daily_stage_and_intermediate,
    target_job=unioned_jobs,
)

dot_graph = generate_dot_graph(jobs, name="finish_line_tuned")
dot_graph.write_dot("finish_line_tuned.dot")  # type: ignore
dot_graph.write_pdf("finish_line_tuned.pdf")  # type: ignore


dot_graph = generate_dot_graph(
    jobs,
    name="finish_line_hourly_job",
)
dot_graph.write_dot("finish_line_final.dot")  # type: ignore
dot_graph.write_pdf("finish_line_final.pdf")  # type: ignore


print(unioned_jobs)
print(updated_targets[daily_stage_and_intermediate.job_id])
