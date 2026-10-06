from pathlib import Path
import subprocess


def main() -> None:
    source = Path("listener.go")
    baseline = source.read_bytes()
    changes = (
        (
            "temporary preservation", "^TestAcceptFaultRecovery/temporary=true$",
            "if errors.As(acceptErr, &networkError) && networkError.Temporary() {",
            "if false && errors.As(acceptErr, &networkError) && networkError.Temporary() {",
            "recovered listener did not accept a connection",
        ),
        (
            "completed retirement", "^TestAcceptFaultRecovery/temporary=false$",
            "closeErr := active.Close()", "var closeErr error",
            "replacement began before service retirement completed",
        ),
        (
            "registration publication", "^TestPendingRegistrationWaiters/closed=false",
            "listener.listener = created", "listener.listener = nil",
            "registration kept a service waiter blocked",
        ),
    )

    try:
        for name, selection, original, replacement, expected in changes:
            text = baseline.decode()
            if text.count(original) != 1:
                raise RuntimeError(f"Mutation target is not unique: {name}")

            source.write_text(text.replace(original, replacement))
            result = subprocess.run(
                ["go", "test", "-race", "-run", selection, "-count=1", "./..."],
                capture_output=True, text=True, timeout=180, check=False,
            )
            output = result.stdout + result.stderr
            print(output, flush=True)
            if result.returncode == 0 or expected not in output:
                raise RuntimeError(f"Mutation did not fail for its intended outcome: {name}")

            source.write_bytes(baseline)
            if source.read_bytes() != baseline:
                raise RuntimeError("Source restoration failed")

            print(f"Mutation proved: {name}", flush=True)
            subprocess.run(
                ["go", "test", "-race", "-run", selection, "-count=1", "./..."],
                timeout=180, check=True,
            )
    finally:
        source.write_bytes(baseline)


if __name__ == "__main__":
    main()
