#!/usr/bin/env python3
"""
Cleanup script to remove all Docker resources created by the LoadTest framework
except the database container (db-container).
"""

import subprocess
import sys


def run_command(command: list[str], capture_output: bool = True) -> tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr."""
    result = subprocess.run(command, capture_output=capture_output, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def get_loadtest_services() -> list[str]:
    """Get all Docker Swarm services related to loadtest."""
    _, stdout, _ = run_command(["docker", "service", "ls", "--format", "{{.Name}}"])
    if not stdout:
        return []
    return [name for name in stdout.split("\n") if name.startswith("loadtest-")]


def get_loadtest_containers() -> list[str]:
    """Get all containers except db-container."""
    _, stdout, _ = run_command([
        "docker", "ps", "-a", "--format", "{{.Names}}"
    ])
    if not stdout:
        return []

    excluded = {"db-container"}
    containers = []
    for name in stdout.split("\n"):
        if name and name not in excluded:
            # Include loadtest-related containers
            if "loadtest" in name.lower():
                containers.append(name)
    return containers


def get_loadtest_images() -> list[str]:
    """Get loadtest Docker images."""
    _, stdout, _ = run_command([
        "docker", "images", "--format", "{{.Repository}}:{{.Tag}}"
    ])
    if not stdout:
        return []
    return [img for img in stdout.split("\n") if img.startswith("loadtest")]


def get_loadtest_networks() -> list[str]:
    """Get loadtest-related networks."""
    _, stdout, _ = run_command(["docker", "network", "ls", "--format", "{{.Name}}"])
    if not stdout:
        return []
    return [net for net in stdout.split("\n") if "loadtest" in net.lower()]


def cleanup_services() -> list[str]:
    """Remove all loadtest Docker Swarm services. Returns list of failures."""
    services = get_loadtest_services()
    if not services:
        print("No loadtest services found.")
        return []

    failures = []
    print(f"Removing {len(services)} service(s)...")
    for service in services:
        print(f"  Removing service: {service}")
        rc, _, stderr = run_command(["docker", "service", "rm", service])
        if rc != 0:
            print(f"    WARNING: Failed to remove service {service}: {stderr}")
            failures.append(service)
    print("Services removed." if not failures else f"Services removed with {len(failures)} failure(s).")
    return failures


def cleanup_containers() -> list[str]:
    """Stop and remove all loadtest containers except db-container. Returns list of failures."""
    containers = get_loadtest_containers()
    if not containers:
        print("No loadtest containers found (excluding db-container).")
        return []

    failures = []
    print(f"Removing {len(containers)} container(s)...")
    for container in containers:
        print(f"  Stopping and removing: {container}")
        run_command(["docker", "stop", container])
        rc, _, stderr = run_command(["docker", "rm", container])
        if rc != 0:
            print(f"    WARNING: Failed to remove container {container}: {stderr}")
            failures.append(container)
    print("Containers removed." if not failures else f"Containers removed with {len(failures)} failure(s).")
    return failures


def cleanup_networks() -> list[str]:
    """Remove loadtest-related networks. Returns list of failures."""
    networks = get_loadtest_networks()
    if not networks:
        print("No loadtest networks found.")
        return []

    failures = []
    print(f"Removing {len(networks)} network(s)...")
    for network in networks:
        print(f"  Removing network: {network}")
        rc, _, stderr = run_command(["docker", "network", "rm", network])
        if rc != 0:
            print(f"    WARNING: Failed to remove network {network}: {stderr}")
            failures.append(network)
    print("Networks removed." if not failures else f"Networks removed with {len(failures)} failure(s).")
    return failures


def cleanup_images(remove_images: bool = False):
    """Remove loadtest Docker images."""
    if not remove_images:
        return

    images = get_loadtest_images()
    if not images:
        print("No loadtest images found.")
        return

    print(f"Removing {len(images)} image(s)...")
    for image in images:
        print(f"  Removing image: {image}")
        run_command(["docker", "rmi", image])
    print("Images removed.")


def prune_unused():
    """Prune unused Docker resources created by loadtest only."""
    print("Pruning unused loadtest containers...")
    run_command(["docker", "container", "prune", "-f",
                 "--filter", "label=com.docker.swarm.service.name=loadtest"])
    print("Pruning unused loadtest networks...")
    _, stdout, _ = run_command(["docker", "network", "ls",
                                "--filter", "name=loadtest", "-q"])
    if stdout:
        for net_id in stdout.split("\n"):
            if net_id.strip():
                run_command(["docker", "network", "rm", net_id.strip()])


def main():
    """Main cleanup function."""
    remove_images = "--images" in sys.argv
    skip_prune = "--no-prune" in sys.argv

    print("=" * 50)
    print("LoadTest Docker Cleanup")
    print("=" * 50)
    print("Note: db-container will be preserved.\n")

    # Cleanup in order: services -> containers -> networks -> images
    all_failures = []
    all_failures.extend(cleanup_services())
    print()

    all_failures.extend(cleanup_containers())
    print()

    all_failures.extend(cleanup_networks())
    print()

    if remove_images:
        cleanup_images(remove_images=True)
        print()

    if not skip_prune:
        prune_unused()
        print()

    print("=" * 50)
    if all_failures:
        print(f"Cleanup finished with {len(all_failures)} failure(s): {', '.join(all_failures)}")
    else:
        print("Cleanup complete!")
    print("=" * 50)

    # Show remaining resources
    print("\nRemaining Docker resources:")
    print("-" * 30)

    _, containers, _ = run_command(["docker", "ps", "-a", "--format", "table {{.Names}}\t{{.Status}}"])
    print("Containers:")
    print(containers if containers else "  None")


if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print("""
LoadTest Docker Cleanup Script

Usage: python cleanup.py [OPTIONS]

Options:
    --images     Also remove loadtest Docker images
    --no-prune   Skip pruning unused Docker resources
    -h, --help   Show this help message

This script removes:
    - All Docker Swarm services starting with 'loadtest-'
    - All containers with 'loadtest' in the name (except db-container)
    - All networks with 'loadtest' in the name
    - Optionally: loadtest Docker images (with --images flag)

The db-container is always preserved to maintain database state.
        """)
        sys.exit(0)

    main()
