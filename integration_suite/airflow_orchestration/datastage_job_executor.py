"""Reusable Airflow-compatible helper for orchestrating IBM DataStage jobs.

Extracted from production automation that coordinated DataStage projects over
SSH, rewritten with placeholders so it can be shared publicly while retaining
original control-flow logic.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import paramiko
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class DataStageJobExecutor:
    """Encapsulates SSH orchestration of DataStage CLI commands."""

    template_fields = ["params"]
    DS_ENGINE_PATH = "/opt/IBM/InformationServer/Server/DSEngine"
    CRED_FILE = "cred_LogTrigger.conf"

    def __init__(self, conn_id: str, project_name: str, job_name: str, params: Dict[str, Any]):
        self.conn_id = conn_id
        self.project_name = project_name
        self.job_name = job_name
        self.params = params
        self.working_directory = f"{self.DS_ENGINE_PATH}/bin/"
        self.logs_directory = "/data/scripts/logs/"
        self.log_file_path = f"{self.logs_directory}{self.project_name}_{self.job_name}.log"
        self.ssh: paramiko.SSHClient | None = None

    # Connection helpers -------------------------------------------------
    def _connect(self) -> paramiko.SSHClient:
        conn = BaseHook.get_connection(self.conn_id)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            allow_agent=False,
            look_for_keys=False,
        )
        logger.info("SSH connection successfully established.")
        return ssh

    # Command builders ---------------------------------------------------
    def _build_params(self) -> str:
        return " ".join(f"-param {key}={value}" for key, value in self.params.items())

    def _build_command(self, sub_command: str) -> str:
        params = self._build_params() if "-run -wait -jobstatus" in sub_command else ""
        return (
            f"cd {self.DS_ENGINE_PATH}; "
            f". ./dsenv; "
            f"cd {self.working_directory}; "
            f"./dsjob -authfile {self.CRED_FILE} "
            f"{sub_command} {params} {self.project_name} {self.job_name}"
        )

    # DataStage actions --------------------------------------------------
    def _exec(self, command: str) -> tuple[int, str]:
        assert self.ssh is not None, "SSH connection is not established"
        logger.debug("Executing command: %s", command)
        _, stdout, stderr = self.ssh.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error = stderr.read().decode()
        if error:
            logger.debug("stderr: %s", error)
        return exit_code, output

    def _logsum(self) -> None:
        logger.info("=== LOG SUMMARY ===")
        _, logsum = self._exec(self._build_command("-logsum"))
        logger.info(logsum)

    def _check_status(self) -> str:
        _, status = self._exec(self._build_command("-jobinfo"))
        logger.info(status)
        return status

    def _reset(self) -> None:
        logger.info("Resetting DataStage job before execution")
        self._exec(self._build_command("-run -mode RESET -wait -jobstatus"))

    def _stop(self) -> None:
        logger.info("Stopping DataStage job")
        self._exec(self._build_command("-stop"))

    def _run(self) -> None:
        logger.info("=== JOB EXECUTION START ===")
        exit_code, _ = self._exec(self._build_command("-run -wait -jobstatus"))
        logger.info("=== JOB EXECUTION END (exit=%s) ===", exit_code)
        self._logsum()
        status = self._check_status()
        if "RUN OK" not in status:
            raise RuntimeError("Job execution failed: RUN OK not found in job info")

    # Public API ---------------------------------------------------------
    def proceed(self, **context: Any) -> None:
        logger.info("Received parameters: %s", self.params)
        try:
            self.ssh = self._connect()
            status = self._check_status()
            if "RUN OK" not in status:
                self._reset()
            self._run()
        except Exception as exc:
            logger.exception("DataStage job failed: %s", exc)
            if self.ssh:
                self._stop()
            raise AirflowFailException("DataStage orchestration failed") from exc
        finally:
            if self.ssh:
                self.ssh.close()
                logger.info("SSH connection closed")
