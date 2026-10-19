# dagster_plus_example

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Measuring staleStatus

`src/dagster_plus_example/defs/stale_scaling.py` defines 1000 layered assets (20 layers x 50, each
depending on two assets of the layer above) to measure how `assetNodes.staleStatus` scales with the
number of assets (dagster-prometheus-exporter#140). It is off by default.

To reproduce:

1. On a branch, set `ENABLED = True` in `stale_scaling.py`. Change `LAYERS` / `PER_LAYER` for another size.
2. Open a PR. The deploy workflow creates a branch deployment, separate from `prod`.
3. In that deployment, run `scale_layers_00`, `scale_layers_05`, `scale_layers_10` and `scale_layers_15`
   in this order. Each is one run of 250 steps on the in-process executor.
4. Point the script at the branch deployment and run it:

   ```bash
   export DAGSTER_GRAPHQL_ENDPOINT=https://<org>.dagster.cloud/<branch-deployment>/graphql
   export DAGSTER_CLOUD_API_TOKEN=...
   python scripts/measure_stale_status.py
   ```

   It prints, for growing prefixes of the graph, the median time of `assetNodes` with and without
   `staleStatus`, and the extra time per asset.
5. Close the PR to remove the branch deployment.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
