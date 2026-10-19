"""Layered asset graph to measure how assetNodes.staleStatus scales with asset count.

LAYERS x PER_LAYER assets. Every asset in layer k depends on two assets of layer k-1,
so each staleStatus needs the latest materialization of the asset and of its parents.

Off by default, so a normal deployment carries none of these assets. To reproduce the
measurement (dagster-prometheus-exporter#140), see "Measuring staleStatus" in the README:
set ENABLED = True on a branch, open a PR to get a branch deployment, run the four
`scale_layers_*` jobs in order, then run scripts/measure_stale_status.py.
"""
import dagster as dg

ENABLED = False
LAYERS = 20
PER_LAYER = 50
LAYERS_PER_JOB = 5


def _name(layer: int, i: int) -> str:
    return f"scale_l{layer:02d}_a{i:02d}"


def _build() -> list[dg.AssetsDefinition]:
    assets = []
    for layer in range(LAYERS):
        for i in range(PER_LAYER):
            deps = (
                []
                if layer == 0
                else [
                    dg.AssetKey(_name(layer - 1, i)),
                    dg.AssetKey(_name(layer - 1, (i + 1) % PER_LAYER)),
                ]
            )

            @dg.asset(name=_name(layer, i), deps=deps, group_name="stale_scaling", code_version="1")
            def _a() -> None:
                return None

            assets.append(_a)
    return assets


@dg.definitions
def defs() -> dg.Definitions:
    if not ENABLED:
        return dg.Definitions()
    assets = _build()
    jobs = [
        dg.define_asset_job(
            f"scale_layers_{start:02d}",
            selection=dg.AssetSelection.assets(
                *[
                    dg.AssetKey(_name(layer, i))
                    for layer in range(start, min(start + LAYERS_PER_JOB, LAYERS))
                    for i in range(PER_LAYER)
                ]
            ),
            executor_def=dg.in_process_executor,
        )
        for start in range(0, LAYERS, LAYERS_PER_JOB)
    ]
    return dg.Definitions(assets=assets, jobs=jobs)
