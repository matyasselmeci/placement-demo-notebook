import datetime
import logging
import math
import os
import pathlib
import sys
import time

import classad2
import htcondor2

from demo.common import TOKEN_FILENAME, TokenState, get_timezone, get_token_state

_log = logging.getLogger(__name__)


class AP:
    """
    AP is a class for interacting with a remote Access Point, specifically its SchedD.
    """

    def __init__(self, collector_host=None, schedd_host=None):
        self.collector_host = collector_host
        if collector_host:
            collector = htcondor2.Collector(collector_host)
        else:
            collector = htcondor2.Collector()
        self.schedd_host = schedd_host or htcondor2.param["SCHEDD_HOST"]
        self.schedd_ad = collector.locate(htcondor2.DaemonType.Schedd, self.schedd_host)
        self.schedd = htcondor2.Schedd(self.schedd_ad)

    def __getstate__(self):
        """
        Return the object's state, removing unpicklable entries -- in this case,
        the handle to the schedd.  We save the schedd ad instead.
        """
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["schedd"]
        return state

    def __setstate__(self, state):
        """
        Restore the object's state from a pickle; recreate the handle to the
        schedd using the schedd ad we saved.
        """
        # Restore instance attributes
        self.__dict__.update(state)
        self.schedd = htcondor2.Schedd(self.schedd_ad)

    def place(self, submit_object: htcondor2.Submit) -> "Placement":
        try:
            submit_result = self.schedd.submit(submit_object, spool=True)
        except htcondor2.HTCondorException as err:
            if "errmsg=AUTHENTICATE" in str(err):
                raise RuntimeError(
                    "Authentication to the AP failed. You might need to get another token."
                )
            else:
                raise
        self.schedd.spool(submit_result)
        placement = Placement(submit_result, ap=self)
        return placement

    def query(
        self,
        constraint: classad2.ExprTree | str = "True",
        attributes: list[str] | None = None,
    ) -> list[classad2.ClassAd]:
        return self.schedd.query(constraint=constraint, projection=attributes or [])

    def get_job_count(self, constraint: classad2.ExprTree | str = "True") -> int:
        return len(
            self.query(constraint=constraint, attributes=["ClusterId", "ProcId"])
        )

    def show_job_count(self, constraint: classad2.ExprTree | str = "True"):
        count = self.get_job_count(constraint)
        if constraint == "True":
            print("There are %d jobs currently placed at the AP." % count)
        else:
            print(
                "There are %d jobs matching the constraint %s currently placed at the AP"
                % (count, constraint)
            )

    def describe_token(self, token_filename: str = TOKEN_FILENAME):
        project = None
        user = None
        have_read = False
        have_write = False
        ad = {}
        text = []

        state = get_token_state(token_filename)

        match state:
            case TokenState.MISSING:
                print("The token file is missing")
                return
            case TokenState.UNREADABLE:
                print("The token file cannot be read or is not a recognizable token")
                return
            case TokenState.EXPIRED:
                print("The token is expired.")
                return
            case TokenState.OK:
                pass

        try:
            ad = htcondor2.ping(self.schedd_ad, "READ")
            have_read = True
            # ^^ maybe also check ad['AuthorizationSucceeded'] ?
            text.append(
                "You can list jobs and view the details of jobs with your current token."
            )
        except htcondor2.HTCondorException as err:
            if "Failed to start command" in str(err):
                have_read = False
                text.append(
                    "You CANNOT list jobs or view the details of jobs with your current token."
                )
            else:
                raise
        try:
            ad = htcondor2.ping(self.schedd_ad, "WRITE")
            have_write = True
            # ^^ maybe also check ad['AuthorizationSucceeded'] ?
            user_ad = (self.schedd.queryUserAds(constraint=f'User=="{user}"') or [{}])[0]  # fmt: skip
            # ^^ TODO We can't do this query if we don't have READ.
            if user_ad.get("Enabled", True):
                text.append(
                    "You can place, remove, edit, hold, release, and otherwise manipulate jobs with your current token."
                )
            else:
                text.append(
                    "You can remove, edit, hold, release, and otherwise manipulate existing jobs with your current token."
                )
                text.append(
                    "However, you CANNOT place new jobs, and your existing jobs will not start."
                )
        except htcondor2.HTCondorException as err:
            if "Failed to start command" in str(err):
                have_read = False
                text.append(
                    "You CANNOT place, remove, edit, hold, release, or otherwise manipulate jobs with your current token."
                )
            else:
                raise
        project = ad.get("AuthTokenProject")
        user = ad.get("MyRemoteUserName")
        if user:
            text.append(f"Your AP User ID is '{user}'.")
        else:
            text.append(
                "ERROR: Your AP User ID is unknown."
            )  # XXX how can this happen?
        if project:
            text.append(f"Your currently selected project is '{project}'.")
        else:
            text.append("WARNING: Your currently selected project is unknown.")

        print("\n".join(text))


class PickleableSubmit(htcondor2.Submit):
    # A pickleable htcondor2.Submit
    def __getstate__(self):
        state = dict(
            input=str(self),
            submitMethod=self.getSubmitMethod(),
        )
        return state

    def __setstate__(self, state):
        super().__init__(state["input"])
        self.setSubmitMethod(state["submitMethod"], allow_reserved_values=True)


def load_job_description(submit_file: str | os.PathLike):
    """
    Reads the job description from the given submit file path.
    """
    file_path = pathlib.Path(submit_file)
    return PickleableSubmit(file_path.read_text())


class Placement:
    _log = _log.getChild("Placement")

    MIN_DELAY_BETWEEN_UPDATES = 10.0  # seconds
    # ^^ maybe I should base this on DCDC?
    MAX_STATUS_WAIT = 60.0  # seconds
    HOLD_REASON_CODE_SPOOLING_INPUT = 16
    IN_PROGRESS_STATUSES = [
        "idle",
        "running",
        "transferring_input",
        "transferring_output",
    ]

    def __init__(self, submit_result: htcondor2.SubmitResult, ap: "AP"):
        self.cluster = submit_result.cluster()
        self.num_procs = submit_result.num_procs()
        self.constraint = f"ClusterId == {self.cluster}"
        self.ap = ap
        self.status_last_update = 0.0
        self.status_next_update = 0.0
        self._status = dict(
            idle=0,
            running=0,
            removed=0,
            completed=0,
            held=0,
            transferring_output=0,
            suspended=0,
            transferring_input=0,  # this one is not a real code
        )
        self.tz = get_timezone()
        self._update_status()

    @property
    def status(self) -> dict:
        self._update_status()
        return self._status.copy()

    def _update_status(self, force=False) -> bool:
        """
        Update self.status() with the latest status of the jobs in this
        placement.  Since it's a remote placement, we query the schedd instead
        of using the JobEventLog.  Keep track of the last update time in
        self.status_last_update; don't update more frequently than
        MIN_DELAY_BETWEEN_UPDATES, unless `force` is true.

        Return True if we were able to update, False otherwise.
        """
        now = time.time()
        if not force and now < self.status_next_update:
            self._log.debug("Not updating status yet -- too soon")
            return False
        try:
            query = self.ap.query(self.constraint, ["JobStatus", "HoldReasonCode"])
        except htcondor2.HTCondorException:
            self._log.warning("Unable to update status", exc_info=True)
            return False

        for code, name in enumerate(self._status.keys(), start=1):
            self._status[name] = 0
            for job in query:
                hold_reason_code = job.get("HoldReasonCode")
                if name == "transferring_input":
                    if (
                        job["JobStatus"] == 5
                        and hold_reason_code == self.HOLD_REASON_CODE_SPOOLING_INPUT
                    ):
                        self._status[name] += 1
                elif job["JobStatus"] == code:
                    if not (
                        name == "held"
                        and hold_reason_code == self.HOLD_REASON_CODE_SPOOLING_INPUT
                    ):
                        self._status[name] += 1
        self.status_last_update = time.time()
        self.status_next_update = time.time() + self.MIN_DELAY_BETWEEN_UPDATES
        return True

    def show_status(self):
        """
        Print the status of jobs in the cluster from this placement.
        """
        self._update_status()
        if not self.status_last_update:
            print("Status unknown")
            return
        update_datetime = datetime.datetime.fromtimestamp(
            self.status_last_update, tz=self.tz
        )
        update_time_str = update_datetime.strftime("%T")
        print(f"As of {update_time_str}:")
        for status_name, num_in_status in self._status.items():
            space_name = status_name.replace("_", " ")
            if num_in_status > 1:
                print(f"{num_in_status} jobs are {space_name}.")
            elif num_in_status == 1:
                print(f"1 job is {space_name}.")

    def show_job_ids(self):
        """
        Print the range of job IDs in this cluster.
        """
        if self.num_procs == 1:
            print("Placement has job ID {0}".format(self.cluster))
        else:
            print(
                "Placement has job IDs {0}.0 - {0}.{1}".format(
                    self.cluster, self.num_procs - 1
                )
            )

    def monitor_jobs(self, minutes: int | float = math.inf):
        """
        Loop and wait until all jobs in this placement are no longer in
        progress.  In progress means 'idle', 'running', 'transferring input',
        or 'transferring output', but not 'held', 'completed', 'removed', or
        'suspended'.

        Exit early if we haven't gotten a status update
        in MAX_STATUS_WAIT.
        """
        minutes = float(minutes)
        if minutes < 0.0:
            raise ValueError(f"'minutes' cannot be negative: {minutes}")
        if math.isinf(minutes):
            print("Monitoring jobs")
        elif minutes == 0.0:
            print(f"Checking job status")
            self._update_status(force=True)
            return self.show_status()
        else:
            print(f"Monitoring jobs for up to {minutes:.2g} minutes")
        end_time = time.time() + minutes * 60.0

        # Monitor jobs until the following:
        #   - no update has been received in MAX_STATUS_WAIT seconds
        #   - no jobs in progress (IN_PROGRESS_STATUS)
        #   - the next update would be after end_time
        while True:
            update_success = self._update_status()
            now = time.time()
            time_since_last_update = self.status_last_update - now
            if time_since_last_update > self.MAX_STATUS_WAIT:
                print(f"No update received in {int(time_since_last_update)} seconds.")
                print(f"Stopped monitoring early; please investigate.")
                return
            if update_success:
                print("---------")
                self.show_status()
                if not self.jobs_in_progress:
                    print("No jobs in progress; done monitoring.")
                    return
            if self.status_next_update > end_time:
                print("End time reached; stopped monitoring.")
                return
            time.sleep(self.MIN_DELAY_BETWEEN_UPDATES)

    @property
    def jobs_in_progress(self) -> int:
        """
        The number of jobs still 'in progress'.
        Does _not_ pull the new status from the schedd.
        """
        return sum(
            self._status[status_name] for status_name in self.IN_PROGRESS_STATUSES
        )

    def retrieve_results(self) -> bool:
        """
        Retrieve results for completed jobs.
        Return True if we didn't get any exceptions from HTCondor.

        Hard to get other information (like how many jobs' results were retrieved)
        because HTCondor does not give us that.  We could try to figure that out
        by querying the schedd, getting the list of expected files, and checking them
        out ourselves but that would be inaccurate since the status of the queue
        would be different between retrieval and query...
        """
        try:
            self.ap.schedd.retrieve(job_spec=self.constraint)
        except htcondor2.HTCondorException as err:
            print(
                f"Retreiving results failed with error message {err}", file=sys.stderr
            )
            return False
        print("Retrieving results successful")
        return True
