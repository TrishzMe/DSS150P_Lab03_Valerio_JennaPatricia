"""Exceptions for pipeline/system failures.

Data-quality problems are not exceptions: they are quarantined with a reason.
These classes are for failures that must stop a stage (and fail its Airflow task).
"""


class PipelineStageError(RuntimeError):
    """A stage failed. Names the stage and run so the failure is diagnosable from one line."""

    def __init__(self, stage: str, run_id: str, cause: BaseException):
        super().__init__(
            f'stage "{stage}" failed for pipeline_run_id={run_id}: {type(cause).__name__}: {cause}'
        )
        self.stage = stage
        self.run_id = run_id


class DataValidationError(RuntimeError):
    """Output violates the data contract (for example duplicate keys or invalid amounts)."""

    def __init__(self, errors: list[str]):
        super().__init__(f'{len(errors)} validation error(s): ' + ' | '.join(errors))
        self.errors = errors


class StageTerminated(RuntimeError):
    """The process received SIGTERM (for example an Airflow execution_timeout)."""
