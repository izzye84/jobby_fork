import os
from typing import Tuple, List

from dbt_cloud import DBTCloud
from operations import generate_dot_graph
from schemas import Job, Model
from dag import DAG
from manifest import Manifest


from operations import distribute_job

api_key = os.environ.get("API_KEY")

if api_key is None:
    raise Exception("An API_KEY env var must be provided.")

dbt_cloud = DBTCloud(account_id=10767, api_key=api_key)
manifest = Manifest(manifest_path="finish_line/manifest.json")


# Daily jobs
daily_stage_and_intermediate = dbt_cloud.get_job(job_id=43696, dag=manifest.dag)
daily_oc11_base_cloud = dbt_cloud.get_job(job_id=43020, dag=manifest.dag)
daily_eom_base_cloud = dbt_cloud.get_job(job_id=43018, dag=manifest.dag)

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
    snapshots_hourly
]

# dot_graph = generate_dot_graph(hourly_jobs, name="finish_line")
# dot_graph.write_dot("finish_line_hourly.dot")  # type: ignore
# dot_graph.write_pdf("finish_line_hourly.pdf")  # type: ignore

# daily_oc11_base = Job(
#     job_id=43020,
#     dag=manifest.dag,
#     name="Daily - OC11 - Base",
#     selector="tag:source_oc11,tag:layer_base,tag:loader_streamsets",
# )

# daily_eom_base = Job(
#     job_id=43018,
#     dag=manifest.dag,
#     name="Daily - EOM - Base",
#     selector="tag:source_eom,tag:layer_base,tag:loader_streamsets",
# )

# hourly_stage = Job(
#     job_id=43030,
#     dag=manifest.dag,
#     name="Hourly - Stage",
#     selector="tag:layer_stage,tag:refresh_hourly",
#     exclude="source:countwise+",
# )

# hourly_orders_intermediate = Job(
#     job_id=43029,
#     dag=manifest.dag,
#     name="Hourly - Orders - Intermediate",
#     selector="int_current_lens_purchase_orders int_current_lens_purchase_orders_line_item int_current_lens_orders int_current_lens_order_line_item int_current_lens_lpn int_current_lens_lpn_detail int_current_lens_asn int_current_lens_po_ref_fields int_current_lens_charge_detail int_current_lens_pob int_current_lens_purchase_orders_attribute int_current_tax_summary int_current_lens_payment_detail int_current_lens_payment_transaction int_current_lens_invoice int_current_lens_invoice_line int_current_lens_web_profile int_current_lens_riskified int_order_payment_transaction int_order_line int_order_line_fct",
# )

# hourly_marts = Job(
#     job_id=38468,
#     dag=manifest.dag,
#     name="Marts - Hourly",
#     selector="tag:mart,tag:hourly tag:layer_marts,tag:refresh_hourly",
# )


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
    snapshots_hourly
]

# for index, job in enumerate(jobs):
#     cloud_job = jobs_cloud[index]

#     if not set(job.models) == set(cloud_job.models):
#         print(f"The cloud and manual versions of {job.name} and {cloud_job.name} have different models")
#         print(job.selector, job.exclude)
#         print(cloud_job.selector, cloud_job.exclude)


# dot_graph = generate_dot_graph(jobs, name="finish_line")
# dot_graph.write_dot("finish_line_current.dot")  # type: ignore
# dot_graph.write_pdf("finish_line_current.pdf")  # type: ignore

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

# dot_graph.write_dot("finish_line_distributed.dot")  # type: ignore
# dot_graph.write_pdf("finish_line_distributed.pdf")  # type: ignore


# Union together the hourly jobs that have dependencies
jobs_to_union = [
    updated_targets[hourly_stage_cloud.job_id],
    hourly_orders_intermediate_cloud,
    hourly_marts_cloud,
    hourly_countwise,
    stockmaster_timeline,
    customer_dist_order_fct,
    stage_hourly,
    snapshots_hourly
]


unioned_jobs = jobs_to_union[0].union(jobs[1:-1])
unioned_jobs.name = "Hourly Models"
print(unioned_jobs.selector)

exit()


jobs = [
    demand_hour_day_segment_fill_loc,
    finance_sales_validated,
    labor_forcast_and_sales_plan,
    updated_targets[daily_stage_and_intermediate.job_id],
    unioned_jobs,
]

# dot_graph = generate_dot_graph(jobs, name="finish_line_unioned")
# dot_graph.write_dot("finish_line_unioned.dot")  # type: ignore
# dot_graph.write_pdf("finish_line_unioned.pdf")  # type: ignore

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
        'stg_tfl_stock_master',
        'stg_countwise_chains',
        'stg_countwise_sites_in_chain',
        'stg_countwise_location'
    ],
    source_job=daily_stage_and_intermediate,
    target_job=unioned_jobs,
)


# dot_graph = generate_dot_graph(jobs, name="finish_line_tuned")
# dot_graph.write_dot("finish_line_tuned.dot")  # type: ignore
# dot_graph.write_pdf("finish_line_tuned.pdf")  # type: ignore





dot_graph = generate_dot_graph(
    jobs,
    name="finish_line_hourly_job",
)
dot_graph.write_dot("finish_line_final.dot")  # type: ignore
dot_graph.write_pdf("finish_line_final.pdf")  # type: ignore

# print("===== Distribution =====")


# new_jobs, job_1_remainder = distribute_job(manifest.dag, job_1, [job_3])
# new_jobs_2, new_job_3_remainder = distribute_job(manifest.dag, new_jobs[0], [job_2])

# print(new_jobs[0])
# print(new_job_3_remainder)
# print(new_jobs_2[0])
