from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule
from prefect.blocks.system import Secret




# Access the stored secret

def deploy():
    # flow.from_source will actually clone the repository to load the flow
    flow.from_source(
        # Here we are using GitHub but it works for GitLab, Bitbucket, etc.
        source=GitRepository(
            url="https://github.com/DataEngITEX/prefect-data-pipelines.git",
            credentials={
                # We are assuming you have a Secret block named `github-access-token`
                # that contains your GitHub personal access token
                "access_token":Secret.load("github-prefect-data-pipelines-repo-secret",validate=False),
            },
        ),
        entrypoint="pipelines/rca_pipeline/rca_pipeline_entry.py:spark_flow",
    ).deploy(
        name="RCA-PIPELINE",
        schedules=[
            # Run the flow every Monday at 12 PM
            CronSchedule(cron="0 12 * * 1")
        ],
        work_pool_name="production-pool",
        # Define a different default parameter for this deployment
       
    )
 

if __name__ == "__main__":
    deploy()