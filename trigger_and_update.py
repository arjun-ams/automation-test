"""trigger_and_update.py

Actively triggers the GitHub Actions workflow(s) configured in
trausCsvConfig.json (via the workflow_dispatch API), waits for each
triggered run to finish, then performs a full TRAUS CSV refresh using the
freshly collected run data - reusing all of the GitHub/log-scanning logic
from automation.py.

This is the "run it now and update the CSVs with what just happened" tool.
If you just want to refresh the CSVs from whatever runs already exist
(without triggering anything new), use automation.py directly instead.

Usage:
    python trigger_and_update.py [--config trausCsvConfig.json] [--rows 100]
                                  [--trigger-count 3] [--poll-interval 10]
                                  [--timeout 600] [--dry-run]

Prerequisites:
    * A GitHub personal access token is REQUIRED (not optional) for this
      script, set in the environment variable named by github.tokenEnvVar in
      the config file (defaults to "GITHUB_TOKEN"). Triggering a
      workflow_dispatch run needs a token with the "repo" scope (classic) or
      "actions:write" permission (fine-grained) - read-only/public_repo
      scope is not enough to trigger runs, only to read them.
    * The target workflow's YAML must declare "workflow_dispatch:" as one of
      its "on:" triggers, otherwise GitHub will reject the dispatch request.
    * Same trausCsvConfig.json "repos" entries as automation.py (owner,
      repo, branch, workflow, technology), plus an optional "triggerCount"
      on each repo entry (or a top-level "triggerCount" default for all
      repos) controlling how many times that workflow gets triggered.
      --trigger-count on the command line overrides both.

Behaviour:
    * For every configured repo, looks up the workflow by display name (see
      automation.find_workflow_by_name) once, then triggers it N times in a
      row, where N = --trigger-count, else the repo's own "triggerCount",
      else the config's top-level "triggerCount", else 1. Each trigger is
      sequential - it waits for one run to finish before starting the next.
    * Each trigger POSTs a workflow_dispatch event for the configured
      branch, then polls the workflow's runs list every --poll-interval
      seconds (default 10s) looking for the run that was just dispatched
      (matched by event type and created_at >= dispatch time), until it
      reaches a terminal status=="completed" or --timeout seconds (default
      600s) elapse.
    * Once completed, reuses automation.find_artifact_info_from_run_log to
      scrape the real artifact download URL and filename from that run's
      job log, exactly like automation.py does for historical runs.
    * Individual trigger attempts that fail to dispatch, time out, or have
      no artifact download URL in their log are skipped (logged, not
      fatal) - the remaining attempts/repos still proceed.
    * All successfully triggered+collected runs across all configured repos
      and all trigger attempts are then written via
      automation.write_update_csv / automation.write_upload_csv, fully
      overwriting both CSVs (same full refresh / cycle-to-rowsPerCsv
      behaviour as automation.py).
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import automation

logger = logging.getLogger(__name__)


def trigger_workflow_dispatch(session, owner, repo, workflow_id, ref):
    url = f"{automation.GITHUB_API_BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches"
    resp = session.post(url, json={"ref": ref}, timeout=30)
    if resp.status_code == 404:
        raise RuntimeError(
            f"Could not dispatch workflow id={workflow_id} in {owner}/{repo} on ref={ref} (HTTP 404). "
            "Check that the workflow declares 'workflow_dispatch:' as a trigger, that the ref/branch "
            "exists, and that your token has 'repo' or 'actions:write' permission."
        )
    resp.raise_for_status()
    logger.info("Dispatched workflow id=%s in %s/%s on ref=%s", workflow_id, owner, repo, ref)


def find_dispatched_run(session, owner, repo, workflow_id, branch, dispatched_after):
    """
    Looks at the most recent runs for a workflow and returns the earliest
    one that was created at/after dispatched_after (a timezone-aware
    datetime) via a workflow_dispatch event - i.e. the run we just
    triggered. Returns None if nothing matching has shown up yet.
    """
    url = f"{automation.GITHUB_API_BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_id}/runs"
    params = {"per_page": 10, "event": "workflow_dispatch"}
    if branch:
        params["branch"] = branch
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    runs = resp.json().get("workflow_runs", [])

    candidates = []
    for run in runs:
        created_at_str = run.get("created_at")
        if not created_at_str:
            continue
        created_at = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if created_at >= dispatched_after:
            candidates.append(run)

    if not candidates:
        return None
    candidates.sort(key=lambda r: r["created_at"])
    return candidates[0]


def wait_for_dispatched_run(session, owner, repo, workflow_id, branch, poll_interval, timeout):
    """
    Triggers nothing itself (call trigger_workflow_dispatch first) - polls
    until the dispatched run appears and reaches status=="completed", or
    raises TimeoutError after `timeout` seconds.
    """
    dispatched_after = datetime.now(timezone.utc)
    deadline = time.time() + timeout
    last_run = None

    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            run = find_dispatched_run(session, owner, repo, workflow_id, branch, dispatched_after)
        except requests.RequestException as exc:
            logger.warning("Error polling runs for workflow id=%s in %s/%s: %s", workflow_id, owner, repo, exc)
            continue

        if not run:
            logger.info("Waiting for dispatched run to appear for workflow id=%s in %s/%s ...", workflow_id, owner, repo)
            continue

        last_run = run
        status = run.get("status")
        logger.info("Run %s for workflow id=%s in %s/%s: status=%s", run["id"], workflow_id, owner, repo, status)
        if status == "completed":
            logger.info("Run %s completed with conclusion=%s", run["id"], run.get("conclusion"))
            return run

    if last_run:
        raise TimeoutError(
            f"Run {last_run['id']} for workflow id={workflow_id} in {owner}/{repo} did not complete within "
            f"{timeout}s (last status={last_run.get('status')})"
        )
    raise TimeoutError(
        f"No run appeared for the workflow_dispatch triggered on workflow id={workflow_id} in {owner}/{repo} "
        f"within {timeout}s"
    )


def trigger_once_and_build_record(session, owner, repo, branch, workflow_name, technology,
                                   workflow_id, workflow_path, workflow_slug, yaml_upload_path,
                                   default_upload_path, poll_interval, timeout, attempt, trigger_count):
    """
    Triggers a single workflow_dispatch run, waits for it to complete, and
    builds one job record from it. Returns None (and logs why) if the
    trigger, wait, or artifact-log-scan step fails for this attempt.
    """
    logger.info("Triggering run %d/%d for %s/%s workflow %r", attempt, trigger_count, owner, repo, workflow_name)

    try:
        trigger_workflow_dispatch(session, owner, repo, workflow_id, branch or "main")
    except (requests.RequestException, RuntimeError) as exc:
        logger.warning("Attempt %d/%d for %s/%s workflow %r: could not trigger dispatch: %s",
                        attempt, trigger_count, owner, repo, workflow_name, exc)
        return None

    try:
        run = wait_for_dispatched_run(session, owner, repo, workflow_id, branch, poll_interval, timeout)
    except TimeoutError as exc:
        logger.warning("Attempt %d/%d for %s/%s workflow %r: %s", attempt, trigger_count, owner, repo, workflow_name, exc)
        return None

    run_id = run["id"]
    run_number = run.get("run_number", run_id)
    job_name = run.get("display_title") or run.get("name") or f"{repo}-{workflow_slug}-{run_number}"
    build_tag = f"gh-actions-{owner}-{repo}-{workflow_slug}-{run_number}"

    artifact_url, log_filename = automation.find_artifact_info_from_run_log(session, owner, repo, run_id)
    if not artifact_url:
        logger.warning("SKIPPED run %s (attempt %d/%d) for %s/%s workflow %r: no artifact download URL found in its job log",
                        run_id, attempt, trigger_count, owner, repo, workflow_name)
        return None

    upload_path = log_filename or yaml_upload_path or default_upload_path

    logger.info("INCLUDED run %s (attempt %d/%d): jobName=%r jobUrl=%s uploadFilesPath=%s",
                run_id, attempt, trigger_count, job_name, artifact_url, upload_path)

    return {
        "technology": technology,
        "jobName": job_name,
        "buildTag": build_tag,
        "jobUrl": artifact_url,
        "uploadFilesPath": upload_path,
    }


def trigger_and_collect_records(config: dict, session, poll_interval: int, timeout: int,
                                 trigger_count_override: int = None) -> list:
    """
    For every configured repo: look up its workflow, then trigger it N
    times (sequentially, waiting for each run to complete before starting
    the next), building one job record per successful run (reusing
    automation's artifact/log-scanning logic).

    N is resolved per repo as: --trigger-count CLI override, else the
    repo's own "triggerCount", else the config's top-level "triggerCount",
    else 1.

    Repos whose workflow can't be found are skipped entirely; individual
    trigger attempts that fail to dispatch, time out, or have no artifact
    URL in the log are skipped (logged, not fatal) without aborting the
    remaining attempts/repos.
    """
    default_upload_path = config.get("defaultUploadFilesPath", "pom.xml")
    default_trigger_count = config.get("triggerCount", 1)

    records = []
    for repo_cfg in config.get("repos", []):
        owner = repo_cfg["owner"]
        repo = repo_cfg["repo"]
        branch = repo_cfg.get("branch") or None
        workflow_name = repo_cfg["workflow"]
        technology = repo_cfg.get("technology", "Java")
        trigger_count = trigger_count_override or repo_cfg.get("triggerCount", default_trigger_count)

        logger.info("Processing repo %s/%s (branch=%s, workflow=%r, triggerCount=%d)",
                    owner, repo, branch or "<default>", workflow_name, trigger_count)

        try:
            workflow = automation.find_workflow_by_name(session, owner, repo, workflow_name)
        except requests.RequestException as exc:
            logger.warning("Skipping %s/%s: could not list workflows: %s", owner, repo, exc)
            continue

        if not workflow:
            logger.warning("No workflow named %r found in %s/%s", workflow_name, owner, repo)
            continue

        workflow_id = workflow["id"]
        workflow_path = workflow.get("path", "")
        workflow_slug = automation.slugify(Path(workflow_path).stem) if workflow_path else automation.slugify(workflow_name)

        try:
            yaml_text = automation.get_workflow_yaml_text(session, owner, repo, workflow_path, branch) if workflow_path else None
        except requests.RequestException as exc:
            logger.warning("Could not read workflow yaml for %s/%s/%s: %s", owner, repo, workflow_path, exc)
            yaml_text = None
        yaml_upload_path = automation.extract_upload_path(yaml_text, None)

        repo_records_before = len(records)
        for attempt in range(1, trigger_count + 1):
            record = trigger_once_and_build_record(
                session, owner, repo, branch, workflow_name, technology,
                workflow_id, workflow_path, workflow_slug, yaml_upload_path,
                default_upload_path, poll_interval, timeout, attempt, trigger_count,
            )
            if record:
                records.append(record)

        logger.info("Repo %s/%s: %d of %d triggered run(s) included",
                    owner, repo, len(records) - repo_records_before, trigger_count)

    return records


def parse_args():
    parser = argparse.ArgumentParser(
        description="Trigger configured GitHub Actions workflows, wait for completion, and refresh TRAUS CSVs."
    )
    parser.add_argument("--config", type=Path, default=automation.DEFAULT_CONFIG_PATH, help="Path to trausCsvConfig.json")
    parser.add_argument("--update-csv", type=Path, default=automation.DEFAULT_UPDATE_CSV, help="Path to truasUpdateTestData.csv")
    parser.add_argument("--upload-csv", type=Path, default=automation.DEFAULT_UPLOAD_CSV, help="Path to truasUploadTestData.csv")
    parser.add_argument("--rows", type=int, default=None, help="Override rowsPerCsv from config")
    parser.add_argument("--trigger-count", type=int, default=None,
                         help="How many times to trigger each configured workflow (default: each repo's own "
                              "'triggerCount', or the config's top-level 'triggerCount', or 1)")
    parser.add_argument("--poll-interval", type=int, default=10, help="Seconds between polls while waiting for a run to complete (default 10)")
    parser.add_argument("--timeout", type=int, default=600, help="Max seconds to wait for each triggered run to complete (default 600 = 10 minutes)")
    parser.add_argument("--dry-run", action="store_true", help="Trigger and wait as normal, but don't write the CSVs")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = parse_args()
    config = automation.load_config(args.config)
    rows_per_csv = args.rows if args.rows is not None else config.get("rowsPerCsv", 100)

    github_cfg = config.get("github", {})
    token_env_var = github_cfg.get("tokenEnvVar", "GITHUB_TOKEN")
    if not os.getenv(token_env_var):
        logger.error("GitHub token not found in %s. Triggering workflow_dispatch requires a token with "
                      "'repo' or 'actions:write' permission - unauthenticated requests cannot trigger runs.",
                      token_env_var)
        sys.exit(1)

    logger.info("Starting trigger-and-refresh: rowsPerCsv=%d pollInterval=%ds timeout=%ds dryRun=%s",
                rows_per_csv, args.poll_interval, args.timeout, args.dry_run)

    session = automation.build_session(config)
    records = trigger_and_collect_records(config, session, args.poll_interval, args.timeout, args.trigger_count)
    logger.info("Collected %d usable workflow run record(s) across %d configured repo(s)",
                len(records), len(config.get("repos", [])))

    rows = automation.cycle_to_length(records, rows_per_csv)

    traus_destination = config.get("trausDestination", "Jira")
    traus_properties = config.get("trausProperties", "no")
    jira_test_case_ids = config.get("jiraTestCaseIds", [])

    automation.write_update_csv(args.update_csv, rows, traus_destination, args.dry_run)
    automation.write_upload_csv(args.upload_csv, rows, traus_destination, traus_properties, jira_test_case_ids, args.dry_run)

    logger.info("Done. %d row(s) written to each CSV (cycled from %d triggered record(s)).", len(rows), len(records))


if __name__ == "__main__":
    main()
