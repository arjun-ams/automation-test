"""automation.py

Refreshes the TRAUS performance test data feeders used by the Gatling simulations:
    - truasUpdateTestData.csv (written next to this script by default)
    - truasUploadTestData.csv (written next to this script by default)

These CSVs used to be hand-maintained with static Jenkins job URLs. Those Jenkins
builds eventually age out / get purged, which leaves TRAUS pointing at job URLs
that no longer exist. This script instead pulls real, currently existing GitHub
Actions workflow runs (from the repos configured in trausCsvConfig.json, on
public github.com - not GitHub Enterprise) and regenerates both CSVs from that
live data, so TRAUS always has valid job references to work against.

Usage:
    python automation.py [--config trausCsvConfig.json] [--rows 100]
                          [--runs-per-workflow 25] [--dry-run]

Prerequisites:
    * (Optional but recommended) Set a GitHub personal access token in the
      environment variable named by github.tokenEnvVar in the config file
      (defaults to "GITHUB_TOKEN"). Public repos work without a token too,
      but unauthenticated requests are limited to 60/hour by GitHub.
    * Edit trausCsvConfig.json and list the repos you want test data sourced
      from. Each entry only needs four things:
          "owner"    - the GitHub account/org, e.g. "arjun-ams"
          "repo"     - the repository name, e.g. "workflow"
          "branch"   - the branch to pull runs from, e.g. "main"
          "workflow" - the workflow's display name, e.g. "sample workflow"

Behaviour:
    * For every configured repo, the script looks up the workflow by its
      display name via the GitHub "list repository workflows" API (exact
      match first, falling back to a case-insensitive substring match).
    * It then fetches the most recent completed runs (status=completed) for
      that workflow on the configured branch via the GitHub Actions API.
    * jobName is taken from the run's display title (the text GitHub Actions
      shows as the run's name in the UI - usually the triggering commit
      message or a custom run-name).
    * jobUrl is the real artifact download URL, scraped from the run's own
      job log - specifically the line actions/upload-artifact prints on a
      successful upload, e.g.
          Artifact download URL: https://github.com/arjun-ams/workflow/actions/runs/37486458775/artifacts/12345
      Reading job logs requires a GitHub token (see Prerequisites above) -
      if the log can't be read, or the workflow never actually uploaded an
      artifact, that run is skipped entirely (no jobUrl is fabricated).
    * The artifact filename (uploadFilesPath) is found first by scanning the
      run's own job log (many workflows print it via $GITHUB_OUTPUT/echo
      rather than actually uploading an artifact), falling back to
      best-effort scanning the workflow's YAML file for an
      actions/upload-artifact step and reading its "path:" value.
    * All collected runs across all configured repos are pooled together and
      cycled through to produce exactly rowsPerCsv rows per file (if fewer
      runs exist than rows requested, runs are repeated/cycled).
    * Both CSV files are fully overwritten (header + data rows) on every run,
      so stale rows never accumulate.
"""
import argparse
import base64
import csv
import itertools
import json
import logging
import os
import random
import re
import sys
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "trausCsvConfig.json"
DEFAULT_UPDATE_CSV = SCRIPT_DIR / "truasUpdateTestData.csv"
DEFAULT_UPLOAD_CSV = SCRIPT_DIR / "truasUploadTestData.csv"

GITHUB_API_BASE = "https://api.github.com"
GITHUB_WEB_BASE = "https://github.com"

UPDATE_CSV_HEADER = ["trausDestination", "jobName", "Technology", "jobUrl"]
UPLOAD_CSV_HEADER = [
    "Technology", "trausDestination", "jobName", "testCaseId",
    "buildTag", "uploadFilesPath", "trausProperties", "testSetId",
]

UPLOAD_ARTIFACT_MARKER = "actions/upload-artifact"


def load_config(config_path: Path) -> dict:
    logger.info("Loading config from %s", config_path.resolve())
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    logger.info("Loaded config: %d repo(s) configured", len(config.get("repos", [])))
    return config


def slugify(text: str) -> str:
    """Turn a workflow name / file stem into a dash-joined identifier fragment."""
    text = re.sub(r"[^A-Za-z0-9]+", "-", text.strip())
    return text.strip("-")


def extract_upload_path(yaml_text: str, default_path: str) -> str:
    """
    Best-effort scan of a workflow YAML for the path: value of an
    actions/upload-artifact step. This is a lightweight regex/line scan
    (no YAML parser dependency) and falls back to default_path when nothing
    usable is found.
    """
    if not yaml_text:
        return default_path

    lines = yaml_text.splitlines()
    for i, line in enumerate(lines):
        if UPLOAD_ARTIFACT_MARKER not in line:
            continue
        for j in range(i + 1, min(i + 8, len(lines))):
            stripped = lines[j].strip()
            if not stripped.startswith("path:"):
                continue
            value = stripped.split("path:", 1)[1].strip().strip('"').strip("'")
            if value and value not in ("|", ">", "|-", ">-"):
                return value.splitlines()[0].strip()
            # Block scalar - the actual path(s) are on the following line(s).
            for k in range(j + 1, min(j + 5, len(lines))):
                nxt = lines[k].strip()
                if nxt:
                    return nxt.strip('"').strip("'")
    return default_path


ARTIFACT_FILENAME_LOG_PATTERN = re.compile(
    r"artifact[_-]?filename\s*[:=]\s*\"?'?([^\s\"'>]+)", re.IGNORECASE
)

# actions/upload-artifact prints a line like:
#   Artifact download URL: https://github.com/OWNER/REPO/actions/runs/RUN_ID/artifacts/ARTIFACT_ID
# when a step in the job actually uploads an artifact.
ARTIFACT_UPLOAD_URL_LOG_PATTERN = re.compile(
    r"(https://github\.com/[^\s\"'<>]+/actions/runs/\d+/artifacts/\d+)", re.IGNORECASE
)


def extract_artifact_filename_from_log(log_text: str):
    """
    Best-effort scan of a job's plain-text log for an artifact filename.
    Many workflows that don't use actions/upload-artifact instead just
    print/export the artifact filename (e.g. via $GITHUB_OUTPUT or a plain
    echo), commonly as "artifact_filename=<name>" or
    "test_artifact_filename=<name>". Returns None if nothing usable is found.
    """
    if not log_text:
        return None
    match = ARTIFACT_FILENAME_LOG_PATTERN.search(log_text)
    if match:
        return match.group(1).strip()
    return None


def extract_artifact_upload_url_from_log(log_text: str):
    """
    Best-effort scan of a job's plain-text log for the real artifact
    download URL that actions/upload-artifact prints on a successful
    upload, e.g.
        https://github.com/OWNER/REPO/actions/runs/RUN_ID/artifacts/ARTIFACT_ID
    Returns None if the log has no such line (e.g. the workflow never
    actually uploaded an artifact).
    """
    if not log_text:
        return None
    match = ARTIFACT_UPLOAD_URL_LOG_PATTERN.search(log_text)
    if match:
        return match.group(1).strip()
    return None


def get_run_jobs(session, owner, repo, run_id):
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def get_job_log_text(session, owner, repo, job_id):
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        logger.warning("Could not fetch job log for job %s in %s/%s (HTTP %s)",
                        job_id, owner, repo, resp.status_code)
        return None
    logger.info("Fetched job log for job %s in %s/%s (%d bytes)", job_id, owner, repo, len(resp.text))
    return resp.text


def find_artifact_info_from_run_log(session, owner, repo, run_id):
    """
    Looks at every job's log for a completed run and tries to pull out both
    the real artifact upload/download URL and an artifact filename (see
    extract_artifact_upload_url_from_log / extract_artifact_filename_from_log).
    Returns a (artifact_url, artifact_filename) tuple; either value may be
    None if no job log yields a match.
    """
    try:
        jobs = get_run_jobs(session, owner, repo, run_id)
    except requests.RequestException as exc:
        logger.warning("Could not list jobs for run %s in %s/%s: %s", run_id, owner, repo, exc)
        return None, None

    logger.info("Run %s in %s/%s has %d job(s); scanning logs for artifact info", run_id, owner, repo, len(jobs))

    artifact_url = None
    artifact_filename = None
    for job in jobs:
        job_id = job.get("id")
        job_name = job.get("name", str(job_id))
        try:
            log_text = get_job_log_text(session, owner, repo, job_id)
        except requests.RequestException as exc:
            logger.warning("Could not fetch log for job %s (%s) in %s/%s: %s", job_id, job_name, owner, repo, exc)
            continue

        if not artifact_url:
            found_url = extract_artifact_upload_url_from_log(log_text)
            if found_url:
                logger.info("Run %s job %r: found artifact download URL in log: %s", run_id, job_name, found_url)
                artifact_url = found_url
            else:
                logger.info("Run %s job %r: no artifact download URL found in this job's log", run_id, job_name)

        if not artifact_filename:
            found_filename = extract_artifact_filename_from_log(log_text)
            if found_filename:
                logger.info("Run %s job %r: found artifact filename in log: %s", run_id, job_name, found_filename)
                artifact_filename = found_filename

        if artifact_url and artifact_filename:
            break

    return artifact_url, artifact_filename


def find_workflow_by_name(session, owner, repo, workflow_name):
    """
    Looks up a workflow defined in a repo by its display name, via the
    GitHub 'list repository workflows' API (paginated). Tries an exact
    (case-insensitive) name match first, then falls back to a substring
    match. Returns the workflow dict, or None if nothing matched.
    """
    workflows = []
    page = 1
    while True:
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/actions/workflows"
        resp = session.get(url, params={"per_page": 100, "page": page}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("workflows", [])
        workflows.extend(batch)
        if len(batch) < 100:
            break
        page += 1

    logger.info("Discovered %d workflow(s) in %s/%s", len(workflows), owner, repo)

    needle = workflow_name.strip().lower()
    for w in workflows:
        if w.get("name", "").strip().lower() == needle:
            logger.info("Matched workflow %r (id=%s, path=%s) by exact name in %s/%s",
                        workflow_name, w.get("id"), w.get("path"), owner, repo)
            return w
    for w in workflows:
        if needle in w.get("name", "").lower():
            logger.info("Matched workflow %r (id=%s, path=%s) by substring in %s/%s",
                        workflow_name, w.get("id"), w.get("path"), owner, repo)
            return w
    logger.warning("No workflow matching %r found among: %s", workflow_name, [w.get("name") for w in workflows])
    return None


def get_workflow_runs(session, owner, repo, workflow_id, per_page, branch=None):
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_id}/runs"
    params = {"status": "completed", "per_page": per_page}
    if branch:
        params["branch"] = branch
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    runs = resp.json().get("workflow_runs", [])
    logger.info("Fetched %d completed run(s) for workflow id=%s in %s/%s%s",
                len(runs), workflow_id, owner, repo, f" on branch {branch}" if branch else "")
    return runs


def get_workflow_yaml_text(session, owner, repo, workflow_path, branch=None):
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{workflow_path}"
    params = {"ref": branch} if branch else None
    resp = session.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        logger.warning("Could not fetch workflow file %s for %s/%s (HTTP %s)",
                        workflow_path, owner, repo, resp.status_code)
        return None
    payload = resp.json()
    if payload.get("encoding") == "base64" and payload.get("content"):
        return base64.b64decode(payload["content"]).decode("utf-8", errors="ignore")
    return None


def build_job_records(config: dict, session) -> list:
    """For every configured repo, look up its workflow by name, fetch its
    GitHub Actions run data, and flatten it into a list of job records
    usable for CSV rows."""
    runs_per_workflow = config.get("runsPerWorkflow", 25)
    default_upload_path = config.get("defaultUploadFilesPath", "pom.xml")

    records = []
    for repo_cfg in config.get("repos", []):
        owner = repo_cfg["owner"]
        repo = repo_cfg["repo"]
        branch = repo_cfg.get("branch") or None
        workflow_name = repo_cfg["workflow"]
        technology = repo_cfg.get("technology", "Java")

        logger.info("Processing repo %s/%s (branch=%s, workflow=%r)", owner, repo, branch or "<default>", workflow_name)

        try:
            workflow = find_workflow_by_name(session, owner, repo, workflow_name)
        except requests.RequestException as exc:
            logger.warning("Skipping %s/%s: could not list workflows: %s", owner, repo, exc)
            continue

        if not workflow:
            logger.warning("No workflow named %r found in %s/%s", workflow_name, owner, repo)
            continue

        workflow_id = workflow["id"]
        workflow_path = workflow.get("path", "")
        workflow_slug = slugify(Path(workflow_path).stem) if workflow_path else slugify(workflow_name)

        try:
            runs = get_workflow_runs(session, owner, repo, workflow_id, runs_per_workflow, branch)
        except requests.RequestException as exc:
            logger.warning("Skipping %s/%s workflow %r: %s", owner, repo, workflow_name, exc)
            continue

        if not runs:
            logger.warning("No completed runs found for %s/%s workflow %r%s", owner, repo, workflow_name,
                            f" on branch {branch}" if branch else "")
            continue

        try:
            yaml_text = get_workflow_yaml_text(session, owner, repo, workflow_path, branch) if workflow_path else None
        except requests.RequestException as exc:
            logger.warning("Could not read workflow yaml for %s/%s/%s: %s", owner, repo, workflow_path, exc)
            yaml_text = None

        # Fallback artifact path if the workflow YAML has an actions/upload-artifact
        # step; used only when a run's own log doesn't reveal an artifact filename.
        yaml_upload_path = extract_upload_path(yaml_text, None)

        repo_records_before = len(records)
        for run in runs:
            run_id = run["id"]
            run_number = run.get("run_number", run_id)
            # display_title is what GitHub Actions shows as the run's name in the UI.
            job_name = run.get("display_title") or run.get("name") or f"{repo}-{workflow_slug}-{run_number}"
            build_tag = f"gh-actions-{owner}-{repo}-{workflow_slug}-{run_number}"

            logger.info("Processing run %s (run_number=%s) for %s/%s workflow %r",
                        run_id, run_number, owner, repo, workflow_name)

            artifact_url, log_filename = find_artifact_info_from_run_log(session, owner, repo, run_id)
            if not artifact_url:
                logger.warning("SKIPPED run %s for %s/%s workflow %r: no artifact download URL found in its job log",
                                run_id, owner, repo, workflow_name)
                continue
            job_url = artifact_url
            upload_path = log_filename or yaml_upload_path or default_upload_path

            logger.info("INCLUDED run %s: jobName=%r jobUrl=%s uploadFilesPath=%s",
                        run_id, job_name, job_url, upload_path)

            records.append({
                "technology": technology,
                "jobName": job_name,
                "buildTag": build_tag,
                "jobUrl": job_url,
                "uploadFilesPath": upload_path,
            })

        logger.info("Repo %s/%s: %d of %d run(s) included after log scan",
                    owner, repo, len(records) - repo_records_before, len(runs))

    return records


def cycle_to_length(records: list, count: int) -> list:
    if not records:
        raise ValueError("No workflow run records were collected; cannot generate CSV rows. "
                          "Check trausCsvConfig.json repos/workflow names and your API token.")
    pool = itertools.cycle(records)
    return [next(pool) for _ in range(count)]


def confirm_file_written(path: Path):
    """Verifies a file actually landed on disk after writing and logs its
    absolute path and size, so it's obvious when/why a CSV didn't appear."""
    resolved = path.resolve()
    if not resolved.exists():
        logger.error("Expected to write %s but the file does not exist after writing!", resolved)
        return
    logger.info("Confirmed file written: %s (%d bytes)", resolved, resolved.stat().st_size)


def write_update_csv(path: Path, rows: list, traus_destination: str, dry_run: bool):
    resolved = path.resolve()
    if dry_run:
        logger.info("[dry-run] Would write %s (%d rows)", resolved, len(rows))
        return
    logger.info("Writing %s (%d rows)", resolved, len(rows))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(UPDATE_CSV_HEADER)
        for r in rows:
            writer.writerow([traus_destination, r["jobName"], r["technology"], r["jobUrl"]])
    confirm_file_written(path)


def write_upload_csv(path: Path, rows: list, traus_destination: str, traus_properties: str,
                      jira_test_case_ids: list, dry_run: bool):
    resolved = path.resolve()
    if dry_run:
        logger.info("[dry-run] Would write %s (%d rows)", resolved, len(rows))
        return
    logger.info("Writing %s (%d rows)", resolved, len(rows))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(UPLOAD_CSV_HEADER)
        for r in rows:
            test_case_id = random.choice(jira_test_case_ids) if jira_test_case_ids else ""
            writer.writerow([
                r["technology"], traus_destination, r["jobName"], test_case_id,
                r["buildTag"], r["uploadFilesPath"], traus_properties, "",
            ])
    confirm_file_written(path)


def resolve_github_token(config: dict) -> str:
    """
    Public github.com only needs a single personal access token (optional -
    unauthenticated requests work for public repos, just at a much lower
    rate limit of 60/hour vs 5000/hour authenticated).
    """
    github_cfg = config.get("github", {})
    token_env_var = github_cfg.get("tokenEnvVar", "GITHUB_TOKEN")
    token = os.getenv(token_env_var)
    if not token:
        logger.warning("No GitHub token found in %s; continuing unauthenticated (rate-limited to 60 requests/hour). "
                        "Note: reading job logs for artifact URLs requires a token even on public repos.",
                        token_env_var)
    else:
        masked = f"{token[:4]}...{token[-4:]}" if len(token) > 8 else "****"
        logger.info("Using GitHub token from %s (%s)", token_env_var, masked)
    return token


def build_session(config: dict) -> requests.Session:
    token = resolve_github_token(config)

    session = requests.Session()
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    session.headers.update(headers)
    return session


def parse_args():
    parser = argparse.ArgumentParser(description="Refresh TRAUS test data CSVs from live GitHub Actions runs.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to trausCsvConfig.json")
    parser.add_argument("--update-csv", type=Path, default=DEFAULT_UPDATE_CSV, help="Path to truasUpdateTestData.csv")
    parser.add_argument("--upload-csv", type=Path, default=DEFAULT_UPLOAD_CSV, help="Path to truasUploadTestData.csv")
    parser.add_argument("--rows", type=int, default=None, help="Override rowsPerCsv from config")
    parser.add_argument("--runs-per-workflow", type=int, default=None, help="Override runsPerWorkflow from config")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and log what would be written, but don't write the CSVs")
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    if args.runs_per_workflow is not None:
        config["runsPerWorkflow"] = args.runs_per_workflow
    rows_per_csv = args.rows if args.rows is not None else config.get("rowsPerCsv", 100)

    logger.info("Starting TRAUS CSV refresh: rowsPerCsv=%d runsPerWorkflow=%d dryRun=%s",
                rows_per_csv, config.get("runsPerWorkflow", 25), args.dry_run)

    session = build_session(config)
    records = build_job_records(config, session)
    logger.info("Collected %d usable workflow run record(s) across %d configured repo(s)",
                len(records), len(config.get("repos", [])))

    rows = cycle_to_length(records, rows_per_csv)

    traus_destination = config.get("trausDestination", "Jira")
    traus_properties = config.get("trausProperties", "no")
    jira_test_case_ids = config.get("jiraTestCaseIds", [])

    write_update_csv(args.update_csv, rows, traus_destination, args.dry_run)
    write_upload_csv(args.upload_csv, rows, traus_destination, traus_properties, jira_test_case_ids, args.dry_run)

    logger.info("Done. %d row(s) written to each CSV (cycled from %d unique record(s)).", len(rows), len(records))


if __name__ == "__main__":
    main()
