import socket
import subprocess
import sys
from urllib.error import URLError
from urllib.request import urlopen


def _check_docker() -> tuple[bool, str]:
    try:
        subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True, "docker daemon reachable"
    except Exception as exc:  # noqa: BLE001
        return False, f"docker unavailable: {exc}"


def _check_http(name: str, url: str) -> tuple[bool, str]:
    try:
        with urlopen(url, timeout=5) as response:  # noqa: S310
            if 200 <= response.status < 500:
                return True, f"{name} responded with {response.status}"
            return False, f"{name} unexpected status {response.status}"
    except URLError as exc:
        return False, f"{name} unreachable: {exc}"


def _check_socket(name: str, host: str, port: int) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=5):
            return True, f"{name} reachable on {host}:{port}"
    except OSError as exc:
        return False, f"{name} unreachable on {host}:{port}: {exc}"


def main() -> int:
    checks = [
        _check_docker(),
        _check_http("minio", "http://localhost:9000/minio/health/live"),
        _check_http("iceberg-rest", "http://localhost:8181/v1/config"),
        _check_socket("kafka", "localhost", 9092),
    ]

    failed = False
    for ok, message in checks:
        marker = "OK" if ok else "FAIL"
        print(f"[{marker}] {message}")
        if not ok:
            failed = True

    if failed:
        print("Doctor failed")
        return 1

    print("Doctor OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
