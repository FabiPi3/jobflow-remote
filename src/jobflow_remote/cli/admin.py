import typer
from rich.prompt import Confirm
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.types import force_opt
from jobflow_remote.cli.utils import exit_with_error_msg, loading_spinner, out_console
from jobflow_remote.config import ConfigManager
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
from jobflow_remote.jobs.jobcontroller import JobController

app_admin = typer.Typer(
    name="admin", help="Commands for administering the database", no_args_is_help=True
)
app.add_typer(app_admin)


@app_admin.command()
def reset(
    reset_output: Annotated[
        bool,
        typer.Option(
            "--reset-output",
            "-o",
            help="Also delete all the documents in the current store",
        ),
    ] = False,
    max_limit: Annotated[
        int,
        typer.Option(
            "--max-limit",
            "-max",
            help=(
                "The database will be reset only if the number of Jobs is lower than the specified limit. 0 means no limit"
            ),
        ),
    ] = 25,
    force: force_opt = False,
):
    """
    Reset the jobflow database.
    WARNING: deletes all the data. These could not be retrieved anymore.
    """
    dm = DaemonManager()

    try:
        with loading_spinner(False) as progress:
            progress.add_task(description="Checking the Daemon status...", total=None)
            current_status = dm.check_status()

    except DaemonError as e:
        exit_with_error_msg(
            f"Error while checking the status of the daemon: {getattr(e, 'message', e)}"
        )

    if current_status not in (DaemonStatus.STOPPED, DaemonStatus.SHUT_DOWN):
        exit_with_error_msg(
            f"The status of the daemon is {current_status.value}. "
            "The daemon should not be running while resetting the database"
        )

    if not force:
        cm = ConfigManager()
        project_name = cm.get_project_data().project.name
        text = Text()
        text.append("This operation will ", style="red")
        text.append("delete all the Jobs data ", style="red bold")
        text.append("for project ", style="red")
        text.append(f"{project_name} ", style="red bold")
        text.append("Proceed anyway?", style="red")

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)
    with loading_spinner(False) as progress:
        progress.add_task(description="Resetting the DB...", total=None)
        jc = JobController()
        done = jc.reset(reset_output=reset_output, max_limit=max_limit)
    out_console.print(f"The database was {'' if done else 'NOT '}reset")
