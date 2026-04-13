#!/usr/bin/env python3
"""
Load Test Orchestrator

This module orchestrates load testing scenarios using Docker Swarm.
It reads configuration from main.json or command line param, manages 
PostgreSQL database, schedules tests, and exports results.
"""

import json
import os
import subprocess
import time
import sys
from datetime import datetime, timedelta, timezone
import urllib.request
from src.utils.db import insert_scenario, ensure_schema_migrations
from src.utils.uuid_generator import generate_uuid4
from src.utils.config_validator import validate_config_file
from src.utils.error_logger import log_error, init_error_logger
from src.test_modules.speed_test import estimate_speed_test_duration


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


CONFIG_PATH = "configurations/main.json"    # default configuration path
DOCKER_IMAGE = "loadtest:latest"            # docker image with necessary modules to run tests
DB_CONTAINER_NAME = "db-container"          # Stores results based on schema from docker/init_db.sql
DB_VOLUME_NAME = "load-test"                # Volume where the database write is persisted
DOCKER_NETWORK_NAME = "loadtest-network"    # Docker overlay network for Swarm service communication
ORCHESTRATOR_BUFFER_MINUTES = 5             # Buffer for Docker service deployment + container startup


def load_config(config_path: str = CONFIG_PATH) -> dict:
    """Load configuration from json"""
    with open(config_path, "r") as f:
        return json.load(f)


def wait_for_postgres(max_retries: int = 30, delay: int = 2) -> bool:
    """Wait for PostgreSQL to be ready by attempting connections"""
    print("  Waiting for PostgreSQL to be ready...")
    for attempt in range(max_retries):
        result = subprocess.run(
            ["docker", "exec", DB_CONTAINER_NAME, "pg_isready", "-U", "postgres"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"  PostgreSQL ready after {attempt * delay} seconds")
            return True
        time.sleep(delay)
    print("  Warning: Unable to start postgres container.")
    return False


def start_postgres_container() -> None:
    """Start PostgreSQL container with Docker volume."""
    # Create volume if not exists
    result = subprocess.run(
        ["docker", "volume", "create", DB_VOLUME_NAME],
        capture_output=True,
        text=True,
        timeout=30
    )
    if result.returncode != 0:
        print(f"  Warning: Failed to create volume: {result.stderr}")

    # Check if container is already running
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={DB_CONTAINER_NAME}"],
        capture_output=True,
        text=True,
        timeout=30
    )

    if result.stdout.strip():
        print(f"  Container {DB_CONTAINER_NAME} already running")
        return

    # Remove stopped container if it exists
    result = subprocess.run(
        ["docker", "rm", "-f", DB_CONTAINER_NAME],
        capture_output=True,
        text=True,
        timeout=30
    )
    if result.returncode != 0 and "No such container" not in result.stderr:
        print(f"  Warning: Failed to remove container: {result.stderr}")

    # Start PostgreSQL container
    subprocess.run([
        "docker", "run", "-d",
        "--name", DB_CONTAINER_NAME,
        "-e", "POSTGRES_PASSWORD=postgres",
        "-e", "POSTGRES_DB=postgres",
        "-v", f"{DB_VOLUME_NAME}:/var/lib/postgresql/data",
        "-p", "5432:5432",
        "--network", DOCKER_NETWORK_NAME,
        "-v", f"{os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docker', 'init_db.sql')}:/docker-entrypoint-initdb.d/init_db.sql",
        "postgres:16-alpine"
    ], check=True, timeout=60)

    # Wait for PostgreSQL to be ready using health check
    wait_for_postgres()


def ensure_docker_network() -> None:
    """Ensure Docker overlay network exists for Swarm service communication."""
    # Check if network already exists
    result = subprocess.run(
        ["docker", "network", "ls", "--filter", f"name={DOCKER_NETWORK_NAME}", "--format", "{{.Name}}"],
        capture_output=True,
        text=True,
        timeout=30
    )

    if DOCKER_NETWORK_NAME in result.stdout:
        print(f"  Network {DOCKER_NETWORK_NAME} already exists")
        return

    # Create overlay network for Swarm services (attachable so regular containers can join)
    result = subprocess.run(
        ["docker", "network", "create", "--driver", "overlay", "--attachable", DOCKER_NETWORK_NAME],
        capture_output=True,
        text=True,
        timeout=30
    )

    if result.returncode != 0:
         raise RuntimeError(f"Failed to create overlay network: {result.stderr}")
    else:
        print(f"  Created overlay network: {DOCKER_NETWORK_NAME}")


def init_docker_swarm() -> None:
    """Initialize Docker Swarm if not already active."""
    result = subprocess.run(
        ["docker", "info", "--format", "{{.Swarm.LocalNodeState}}"],
        capture_output=True,
        text=True,
        timeout=30
    )
    swarm_state = result.stdout.strip()
    print(f"  Swarm state: {swarm_state}")

    if swarm_state != "active":
        print("  Initializing Docker Swarm...")
        init_result = subprocess.run(
            ["docker", "swarm", "init", "--advertise-addr", "127.0.0.1"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if init_result.returncode != 0:
            print(f"  Warning: Swarm init failed: {init_result.stderr}")
        else:
            print("  Swarm initialized successfully")
    else:
        print("  Swarm already active")


def deploy_test_service(scenario_id: str, scenario_config: dict, replicas: int = 1) -> str:
    """
    Deploy a Docker Swarm service for running tests in parallel.
    Returns the service name.
    """
    service_name = f"loadtest-{scenario_id[:8]}"
    protocol = scenario_config.get("protocol", "unknown")

    # Environment variables for the container
    env_vars = [
        "-e", f"SCENARIO_ID={scenario_id}",
        "-e", f"SCENARIO_CONFIG={json.dumps(scenario_config)}",
        "-e", "DB_HOST=db-container",
        "-e", "DB_PORT=5432",
        "-e", "DB_NAME=postgres",
        "-e", "DB_USER=postgres",
        "-e", "DB_PASSWORD=postgres",
    ]

    cmd = [
        "docker", "service", "create",
        "--name", service_name,
        "--replicas", str(replicas),
        "--network", DOCKER_NETWORK_NAME,
        "--restart-condition", "none",
        "--cap-add", "NET_RAW",
        "--cap-add", "NET_ADMIN",
    ] + env_vars + [
        DOCKER_IMAGE,
        "python3", "-m", "src.worker", scenario_id
    ]

    subprocess.run(cmd, check=True, timeout=120)
    return service_name


def remove_service(service_name: str) -> None:
    """Remove a Docker Swarm service."""
    result = subprocess.run(
        ["docker", "service", "rm", service_name],
        capture_output=True,
        text=True,
        timeout=60
    )
    if result.returncode != 0:
        print(f"Docker service {service_name} could not be removed.")
    

def cleanup_exited_containers() -> None:
    """Remove exited containers created from the loadtest image."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"ancestor={DOCKER_IMAGE}", "--filter", "status=exited", "-q"],
        capture_output=True, text=True, timeout=30
    )
    container_ids = result.stdout.strip().split("\n")
    container_ids = [cid for cid in container_ids if cid]
    if container_ids:
        subprocess.run(["docker", "rm"] + container_ids, capture_output=True, text=True, timeout=30)


def check_running_services(active_services: list) -> list:
    """Check which services are still running."""
    running = []
    for service_name, scenario_id in active_services:
        result = subprocess.run(
            ["docker", "service", "ps", service_name, "--filter", "desired-state=running", "-q"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.stdout.strip():
            running.append((service_name, scenario_id))
    return running


def check_failed_services(active_services: list) -> list:
    """Check which services have failed tasks."""
    failed = []
    for service_name, scenario_id in active_services:
        result = subprocess.run(
            ["docker", "service", "ps", service_name, "--filter", "desired-state=shutdown", "--format", "{{.CurrentState}}"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if "Failed" in result.stdout or "Rejected" in result.stdout:
            failed.append((service_name, scenario_id))
    return failed


def get_video_runtime(server_url: str, api_key: str, item_id: str) -> timedelta:
    """Get video runtime from Jellyfin API. RunTimeTicks uses 10,000,000 ticks per second."""
    try:
        url = f"{server_url.rstrip('/')}/Items/{item_id}"
        req = urllib.request.Request(url, headers={"X-Emby-Token": api_key})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            runtime_ticks = data.get("RunTimeTicks", 0)
            runtime_seconds = runtime_ticks / 10_000_000
            return timedelta(seconds=runtime_seconds)
    except Exception as e:
        log_error("orchestrate", "get_video_runtime", e, context=f"item_id={item_id}")
        return timedelta(seconds=120)

def calculate_scenario_end_time(scenarios: list) -> datetime:
    """Calculate the absolute end time across all scenarios."""
    max_end_time = datetime.now(timezone.utc)
    for scenario in scenarios:
        if not scenario.get("enabled", False):
            continue
        schedule = scenario.get("schedule", {})
        start_time_raw = schedule["start_time"]
        if start_time_raw == "immediate":
            start_time = datetime.now(timezone.utc)
        else:
            start_time = datetime.fromisoformat(start_time_raw)
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            else:
                start_time = start_time.astimezone(timezone.utc)

        mode = schedule["mode"]
        if mode == "once":
            end_time = start_time
        elif mode == "recurring":
            duration_hours = float(schedule["duration_hours"])
            end_time = start_time + timedelta(hours=duration_hours)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        # Add protocol based durations (if any)
        protocol = scenario.get("protocol", "")
        if protocol == "":
            raise ValueError(f"Protocol field empty")

        if protocol == "speed_test":
            params = scenario.get("parameters", {})
            per_target_seconds = estimate_speed_test_duration(params.get("duration", 10))
            num_targets = len(params.get("target_url", []))
            end_time += timedelta(seconds=per_target_seconds * max(num_targets, 1))
        elif protocol == "streaming":
            params = scenario.get("parameters", {})
            server_url = params.get("server_url", "")
            api_key = params.get("api_key", "")
            item_ids = params.get("item_ids", [])
            for item_id in item_ids:
                vid_duration = get_video_runtime(server_url, api_key, item_id)
                end_time += vid_duration
        elif protocol == "web_browsing":
            urls = scenario.get("parameters", {}).get("target_url", [])
            end_time += timedelta(seconds=len(urls) * 30)
        elif protocol == "voip_sipp":
            params = scenario.get("parameters", {})
            num_calls = params.get("number_of_calls", 1)
            call_dur = params.get("call_duration", 5)
            num_targets = len(params.get("target_url", []))
            end_time += timedelta(seconds=(num_calls * call_dur + 60) * num_targets)

        max_end_time = max(max_end_time, end_time)

    return max_end_time


def orchestrate(config_path: str = CONFIG_PATH):
    """Main orchestration function."""
    print("=" * 60)
    print("Load Test Orchestrator Starting")
    print("=" * 60)

    # Step 1: Load and validate configuration
    print("\n[1/6] Loading and validating configuration...")
    is_valid, errors = validate_config_file(config_path)
    if not is_valid:
        print(f"  Configuration validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"    - {error}")
        raise ConfigurationError(f"Invalid configuration: {len(errors)} error(s) found")
    print("  Configuration is valid")
    config = load_config(config_path)
    config_name = config.get("global_settings", {}).get("name")
    if config_name:
        print(f"  Configuration name: {config_name}")

    # Initialize error logger
    init_error_logger()

    # Step 2: Setup Docker infrastructure
    print("[2/6] Setting up Docker infrastructure...")
    init_docker_swarm()  # Must init swarm before creating overlay network
    ensure_docker_network()
    start_postgres_container()
    ensure_schema_migrations()

    # Step 3: Process enabled scenarios
    print("[3/6] Processing scenarios...")
    scenarios = config.get("scenarios", [])
    active_services = []
    scenario_ids = {}
    scenario_configs = {}  # Store configs for finalization

    for scenario in scenarios:
        if not scenario.get("enabled", False):
            print(f"  Skipping disabled scenario: {scenario.get('id', 'unknown')}")
            continue

        if not scenario.get("expectations", []):
            print(f"  Skipping scenario with no expectations: {scenario.get('id', 'unknown')}")
            continue

        # Generate UUID for scenario
        scenario_id = generate_uuid4()
        scenario_ids[scenario.get("id")] = scenario_id
        scenario_configs[scenario_id] = scenario
        protocol = scenario.get("protocol", "unknown")

        print(f"  Processing scenario: {scenario.get('id')} ({protocol})")
        print(f"    UUID: {scenario_id}")

        # Step 5: Insert scenario into database
        insert_scenario(
            scenario_id=scenario_id,
            protocol=protocol,
            config_snapshot=scenario,
            config_name=config_name
        )
        
        # Deploy Docker Swarm service - worker handles scheduling and execution
        try:
            service_name = deploy_test_service(scenario_id, scenario, replicas=1)
            active_services.append((service_name, scenario_id))
        except subprocess.CalledProcessError as e:
            log_error("orchestrate", "orchestrate", e,
                      context=f"Failed to deploy service for scenario {scenario.get('id')}")
            print(f"  ERROR: Failed to deploy service for {scenario.get('id')}: {e}")
            continue

    # Step 4: Wait for test execution
    print("[4/6] Waiting for workers to start...")

    scenario_end_time = calculate_scenario_end_time(scenarios)
    print(f"  Scenarios end at: {scenario_end_time.isoformat()}")
    
    # Monitor and wait for completion
    print("[5/6] Running tests...")
    end_time = scenario_end_time + timedelta(minutes=ORCHESTRATOR_BUFFER_MINUTES)  # Buffer for Docker startup delays
    try:
        while datetime.now(timezone.utc) <= end_time:
            running_services = check_running_services(active_services)
            failed_services = check_failed_services(active_services)

            if failed_services:
                print(f"\n  Warning: {len(failed_services)} service(s) failed:")
                for service_name, _ in failed_services:
                    print(f"    - {service_name}")

            # Exit if no services running (all completed or failed)
            if not running_services:
                if failed_services:
                    print("\n  All services stopped (some failed)")
                else:
                    print("\n  All services completed successfully")
                break

            print(f"  {len(running_services)} services running...", end="\r")
            time.sleep(10)
        else:
            print("  Test duration completed (timeout)")

    except KeyboardInterrupt:
        print("\n  Interrupted by user")

    # Step 6: Cleanup services (workers handle their own finalization)
    print("[6/6] Cleaning up services...")
    for service_name, _ in active_services:
        remove_service(service_name)
    cleanup_exited_containers()

    print("\n" + "=" * 60)
    print("Orchestration Complete")
    print("=" * 60)


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else CONFIG_PATH
    orchestrate(config_path)
    
