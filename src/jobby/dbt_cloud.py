from typing import Dict, List

import requests
from loguru import logger


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(self, account_id: int, api_key: str) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._manifests: Dict = {}

    def _check_for_creds(self):
        """Confirm the presence of credentials"""
        if not self._api_key:
            raise Exception("An API key is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise Exception("An account_id is required to get dbt Cloud jobs.")

    def get_latest_job_runs(self, job_id: int) -> Dict:
        """Obtain the most recent run for a job"""
        offset = 0

        run = None

        while True:
            parameters = {
                "offset": offset,
                "job_definition_id": job_id,
                "order_by": "-id",
            }

            response = requests.get(
                url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/runs/",
                params=parameters,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
            )

            run_data = response.json()

            for specficic_run in run_data["data"]:
                if specficic_run["is_success"]:
                    logger.debug(f"Using run {specficic_run['id']}")
                    run = specficic_run
                    break

            if run is not None:
                break

            if (
                run_data["extra"]["filters"]["limit"]
                + run_data["extra"]["filters"]["offset"]
                >= run_data["extra"]["pagination"]["total_count"]
            ):
                break

            offset += run_data["extra"]["filters"]["limit"]

        if run is None:
            raise Exception("Unable to find a latest run.")

        return run

    def get_latest_manifest(self, environemnt_id: int) -> Dict:
        """Return the most recently generated manifest.json file for an environment"""

        # Find the most recent run for an environemnt.

        self._check_for_creds()

        jobs = self.get_jobs(environemnt_id)

        if len(jobs) == 0:
            raise Exception("No recent jobs found.")

        run = self.get_latest_job_runs(jobs[0]["id"])

        manifest_response = requests.get(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/runs/{run['id']}/artifacts/manifest.json",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

        return manifest_response.json()

    def get_jobs(self, environment_id: int) -> List[Dict]:
        """Return a list of Jobs for all the dbt Cloud jobs in an environment."""

        self._check_for_creds()

        offset = 0

        jobs = []

        while True:
            parameters = {"offset": offset}

            # if project_id:
            #     parameters['project_id'] = project_id

            response = requests.get(
                url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/",
                params=parameters,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
            )

            job_data = response.json()
            jobs.extend(job_data["data"])

            if (
                job_data["extra"]["filters"]["limit"]
                + job_data["extra"]["filters"]["offset"]
                >= job_data["extra"]["pagination"]["total_count"]
            ):
                break

            offset += job_data["extra"]["filters"]["limit"]

        return list(filter(lambda x: x["environment_id"] == environment_id, jobs))

    def get_job(self, job_id: int) -> Dict:
        """Generate a Job based on a dbt Cloud job."""

        self._check_for_creds()

        response = requests.get(
            url=(
                f"https://cloud.getdbt.com/api/v2/accounts/"
                f"{self.account_id}/jobs/{job_id}"
            ),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        return response.json()["data"]
