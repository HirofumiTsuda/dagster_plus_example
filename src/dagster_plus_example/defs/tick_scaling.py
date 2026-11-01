"""Many schedules that only skip, to measure how ticks(...) scales with the number of schedules.

Measures dagster-prometheus-exporter#86. Each schedule evaluates every minute and returns a
SkipReason, so it writes a tick but launches no run. Off by default; enable it on a branch only.
"""
import dagster as dg

ENABLED = True
N_SCHEDULES = 300


@dg.op
def tick_scaling_noop() -> None:
    return None


@dg.job
def tick_scaling_job():
    tick_scaling_noop()


def _make(i: int) -> dg.ScheduleDefinition:
    @dg.schedule(
        name=f"tick_scaling_{i:03d}",
        cron_schedule="* * * * *",
        job=tick_scaling_job,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    def _s(context):
        return dg.SkipReason("tick scaling measurement")

    return _s


defs = (
    dg.Definitions(jobs=[tick_scaling_job], schedules=[_make(i) for i in range(N_SCHEDULES)])
    if ENABLED
    else dg.Definitions()
)
