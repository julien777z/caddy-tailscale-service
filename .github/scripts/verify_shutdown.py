from pathlib import Path
import subprocess


def main() -> None:
    source = Path("listener.go")
    baseline = source.read_bytes()
    changes = (
        (
            "shared registration",
            "registration := listener.registration",
            "registration := (*serviceRegistration)(nil)",
            "pending registration attempts=",
        ),
        (
            "waiter closure",
            "case <-listener.closed:\n\t\treturn nil, net.ErrClosed\n\tcase <-registration.done:",
            "case <-registration.done:",
            "closure kept a service waiter blocked",
        ),
        (
            "late listener cleanup",
            "if listener.isClosed() && registration.listener != nil {",
            "if false && registration.listener != nil {",
            "late service listener was not closed",
        ),
    )

    try:
        for name, original, replacement, expected in changes:
            text = baseline.decode()
            if text.count(original) != 1:
                raise RuntimeError(f"Mutation target is not unique: {name}")

            source.write_text(text.replace(original, replacement))
            result = subprocess.run(
                ["go", "test", "-race", "-run", "^TestPendingRegistrationClosure$", "-count=1", "./..."],
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
            output = result.stdout + result.stderr
            print(output, flush=True)
            if result.returncode == 0 or expected not in output:
                raise RuntimeError(f"Mutation did not fail for its intended assertion: {name}")

            source.write_bytes(baseline)
            if source.read_bytes() != baseline:
                raise RuntimeError("Source restoration failed")

            print(f"Mutation proved: {name}", flush=True)
            subprocess.run(
                ["go", "test", "-race", "-run", "^TestPendingRegistrationClosure$", "-count=1", "./..."],
                timeout=180,
                check=True,
            )
    finally:
        source.write_bytes(baseline)


if __name__ == "__main__":
    main()
