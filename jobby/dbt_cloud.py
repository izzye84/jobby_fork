from typing import Optional, Dict, List
import re

from pydantic import BaseModel
import requests

from .types.dag import DAG
from .types.job import Job


class DBTCloud:
    def __init__(self, account_id: int, api_key: str) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._jobs: Optional[Dict] = None
        self._runs: Dict = {}
        self._manifests: Dict = {}

    def get_job(self, job_id: int, dag: DAG) -> Job:
        """Generate a Job based on a dbt Cloud job."""
        response = requests.get(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/{job_id}",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

        dbt_cloud_job = response.json()["data"]

        selectors: List[str] = []
        exclusions: List[str] = []
        for execute_step in dbt_cloud_job["execute_steps"]:
            output = re.search(
                r"(\-\-select|\-s|\-m) ([a-zA-Z_0-9\,\+\@\: ]+)", execute_step
            )
            if output is not None:
                groups = output.groups()
                if len(groups) > 0:
                    selectors.append(groups[1])

            output = re.search(
                r"(\-\-exclude|\-e) ([a-zA-Z_0-9\,\+\@\: ]+)", execute_step
            )
            if output is not None:
                groups = output.groups()
                if len(groups) > 0:
                    exclusions.append(groups[1])

        return Job(
            job_id=job_id,
            name=dbt_cloud_job["name"],
            dag=dag,
            selector=" ".join(selectors).rstrip(),
            exclude=" ".join(exclusions).rstrip(),
        )

    # def _get_jobs(self) -> Dict:
    #     jobs = {}

    #     offset = 0

    #     while True:
    #         parameters = {"offset": offset}

    #         # if project_id:
    #         #     parameters['project_id'] = project_id

    #         response = requests.get(
    #             url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/",
    #             params=parameters,
    #             headers={
    #                 "Authorization": f"Bearer {self._api_key}",
    #                 "Content-Type": "application/json",
    #             },
    #         )

    #         job_data = response.json()

    #         for job in job_data["data"]:
    #             jobs[job["id"]] = Job(job, self)

    #         if (
    #             job_data["extra"]["filters"]["limit"]
    #             + job_data["extra"]["filters"]["offset"]
    #             >= job_data["extra"]["pagination"]["total_count"]
    #         ):
    #             break

    #         offset += job_data["extra"]["filters"]["limit"]
    #     return jobs

    def get_runs(self, job_id: int, run_limit=1) -> List[Dict]:
        """Return a list of dbt Cloud Runs for a given Job."""

        if job_id in self._runs:
            return self._runs[job_id]

        response = requests.get(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/runs/",
            params={
                "job_definition_id": job_id,
                "include_related": "job",
                "limit": run_limit,
            },
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

        run_data = response.json()
        self._runs[job_id] = run_data

        return self._runs[job_id]

    def get_run_manifest(self, run_id: int, step: int) -> str:
        """Get the run manifest artifact associated with a given run step."""
        key = (run_id, step)
        if key in self._manifests:
            return self._manifests[(run_id, step)]

        response = requests.get(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/runs/{run_id}/artifacts/manifest.json",
            params={"step": step},
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        self._manifests[(run_id, step)] = response.json()

        return self._manifests[(run_id, step)]

    # def get_jobs(self) -> Dict:
    #     """Return a list of DBT Cloud Jobs from cache. If the cache is not populated, go and grab them."""

    #     if self._jobs is None:
    #         # Get some jobs, yo.
    #         self._jobs = self._get_jobs()

    #     return self._jobs
