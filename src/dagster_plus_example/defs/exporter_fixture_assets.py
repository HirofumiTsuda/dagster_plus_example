# Added for verifying the exporter on Dagster+: a dependency chain (so
# staleStatus can become STALE) and a partitioned asset (issue #141).
import time

from dagster import AssetCheckResult, AssetCheckSeverity, DailyPartitionsDefinition, asset, asset_check


@asset
def upstream_asset():
    return 1


@asset(deps=[upstream_asset])
def downstream_asset():
    return 2


@asset(partitions_def=DailyPartitionsDefinition(start_date="2026-09-25"))
def partitioned_upstream(context):
    return context.partition_key


@asset(partitions_def=DailyPartitionsDefinition(start_date="2026-09-25"), deps=[partitioned_upstream])
def partitioned_downstream(context):
    return context.partition_key


extra_assets = [upstream_asset, downstream_asset, partitioned_upstream, partitioned_downstream]


@asset_check(asset=upstream_asset)
def upstream_check_passes():
    return AssetCheckResult(passed=True)


@asset_check(asset=upstream_asset)
def upstream_check_fails():
    return AssetCheckResult(passed=False, description="Intentional check failure for exporter testing")


@asset(partitions_def=DailyPartitionsDefinition(start_date="2026-09-25"))
def failing_partitioned():
    raise RuntimeError("Intentional backfill failure for exporter testing")


@asset(partitions_def=DailyPartitionsDefinition(start_date="2026-09-25"))
def slow_partitioned():
    time.sleep(120)


@asset_check(asset=upstream_asset)
def upstream_check_warns():
    return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN)


@asset_check(asset=upstream_asset)
def upstream_check_raises():
    raise RuntimeError("Intentional check execution failure")


@asset
def never_materialized():
    pass


@asset_check(asset=never_materialized)
def never_materialized_check():
    return AssetCheckResult(passed=True)
