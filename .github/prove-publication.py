from pathlib import Path
import subprocess

source = Path("listener.go")
baseline = source.read_bytes()
status = subprocess.check_output(["git", "status", "--porcelain"], text=True)
command = ["go", "test", "-race", "-run", "^TestPublicationAfterClosure$", "./..."]
subprocess.run(command, check=True)
needle = b'client, err := node.LocalClient()\n\tif err != nil {\n\t\tcaddy.Log().Error("start Tailscale service address publication", zap.Error(err))\n\n\t\treturn\n\t}'
assert baseline.count(needle) == 1
try:
    source.write_bytes(baseline.replace(needle, b"client, _ := node.LocalClient()"))
    result = subprocess.run(command, text=True, capture_output=True)
    print(result.stdout)
    print(result.stderr)
    assert result.returncode != 0
    assert "publisher started after its node closed" in result.stdout + result.stderr
    print("PROVEN closed-node-publication: regression assertion failed")
finally:
    source.write_bytes(baseline)
assert subprocess.check_output(["git", "status", "--porcelain"], text=True) == status
assert source.read_bytes() == baseline
subprocess.run(command, check=True)
print("RESTORED closed-node-publication: source and passing regression")
