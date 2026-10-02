# Added for verifying the exporter on Dagster+: a dependency chain (so
# staleStatus can become STALE) and a partitioned asset (issue #141).
from dagster import DailyPartitionsDefinition, asset


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
