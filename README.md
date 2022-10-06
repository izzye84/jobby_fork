# jobby
`jobby` is a python library that makes it easier to understand how dbt Cloud jobs interact, and provides an interface for simulating changes to jobs.
This means that you can:
- Simulate adding and removing models within jobs,
  - Perform set operations on models in jobs (union, intersect, etc)
  - Transfer models between jobs
  - Distribute the models from one job to a list of other jobs
- :sparkles: Generate new selectors :sparkles: for a job
- Generate a graph that shows how jobs interact with each other.

## Demo
Click the image below to view a short demonstration of `jobby`.
[![Demo Loom](https://cdn.loom.com/sessions/thumbnails/1317aa1c009d40deb40740d21a1bf347-1665062843441-with-play.gif)](https://www.loom.com/share/1317aa1c009d40deb40740d21a1bf347)

## Getting Started

To get started, install and import `jobby`, then, create a new `Jobby` instance.

```python
from jobby import Jobby
jobby = Jobby(
    account_id=[dbt Cloud account ID], 
    environemnt_id=[ID of the dbt Cloud environment of interest], 
    api_key=os.environ.get('API_KEY')
)
```

Now you can get jobs from dbt Cloud and start manipulating them

```python
jobs = jobby.get_all_jobs()
```

```python
from jobby import operations
dot_graph = operations.generate_dot_graph(jobs.values(), 'Current Job Graph')
dot_graph.write_png('current_graph.png')
```

![current_graph](https://user-images.githubusercontent.com/3269450/194369824-5f2ba5ca-43b6-47b4-9c57-340f1af1e031.png)

