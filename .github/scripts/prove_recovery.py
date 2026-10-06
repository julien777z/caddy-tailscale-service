import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "listener.go"
TEST = "^TestServiceNodeReplacesCachedStartupFailure$"


def run_test() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["go", "test", "-race", "-count=1", "-run", TEST, "."],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> None:
    baseline = SOURCE.read_bytes()
    mutations = [
        (
            "node.Server = replacement.Server",
            "_ = replacement",
            "retry retained the SDK instance with a cached initialization failure",
        ),
        (
            "if node.closed {\n\t\treturn nil, net.ErrClosed\n\t}",
            "if false {\n\t\treturn nil, net.ErrClosed\n\t}",
            "closed lifecycle retried initialization",
        ),
    ]

    for original, faulty, expected_failure in mutations:
        text = baseline.decode()
        if text.count(original) != 1:
            raise RuntimeError("Mutation target does not occur exactly once")

        try:
            SOURCE.write_text(text.replace(original, faulty))
            result = run_test()
            output = result.stdout + result.stderr
            print(output)

            if result.returncode == 0 or expected_failure not in output:
                raise RuntimeError("Regression did not fail for the intended behavior")
        finally:
            SOURCE.write_bytes(baseline)

        if SOURCE.read_bytes() != baseline:
            raise RuntimeError("Source was not restored exactly")

        print(f"Detected regression: {expected_failure}")

    result = run_test()
    print(result.stdout + result.stderr)
    result.check_returncode()
    print("Both mutations detected; exact baseline restored and passing")


if __name__ == "__main__":
    main()
