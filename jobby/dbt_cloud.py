from typing import Dict

import requests

from .types.job import Job


class DBTCloud:
    def __init__(self, account_id: int, api_key: str) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._manifests: Dict = {}

    def get_job(self, job_id: int) -> Job:
        """Generate a Job based on a dbt Cloud job."""

        if not self._api_key:
            raise Exception("An API_KEY env var is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise Exception("An account_id is required to get dbt Cloud jobs.")

        response = requests.get(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/{job_id}",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        return response.json()["data"]
