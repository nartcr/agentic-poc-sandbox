import os
import dataclasses
import pytz  # BOILERPLATE

# BOILERPLATE — ET timezone constant; imported by all components that need ET timestamps (BAC-7)
ET_TZ = pytz.timezone("America/Toronto")


@dataclasses.dataclass(frozen=True)
class Config:  # BOILERPLATE
    """Immutable runtime configuration bound from environment variables."""
    s3_bucket: str
    s3_input_prefix: str
    s3_reports_prefix: str
    s3_errors_prefix: str
    db_secret_id: str
    sns_success_topic_arn: str
    sns_failure_topic_arn: str
    audit_table: str

    @classmethod
    def from_env(cls) -> "Config":  # LOGIC — reads all required env vars; raises KeyError immediately if any are absent
        return cls(
            s3_bucket=os.environ["S3_BUCKET"],
            s3_input_prefix=os.environ["S3_INPUT_PREFIX"],
            s3_reports_prefix=os.environ["S3_REPORTS_PREFIX"],
            s3_errors_prefix=os.environ["S3_ERRORS_PREFIX"],
            db_secret_id=os.environ["DB_SECRET_ID"],
            sns_success_topic_arn=os.environ["SNS_SUCCESS_TOPIC_ARN"],
            sns_failure_topic_arn=os.environ["SNS_FAILURE_TOPIC_ARN"],
            # LOGIC — default matches the fully qualified audit table name from the design
            audit_table=os.environ.get("AUDIT_TABLE", "app.pipeline_audit"),
        )