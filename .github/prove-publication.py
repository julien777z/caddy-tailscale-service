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
    output = result.stdout + result.stderr
    assert "publisher started after its node closed" in output or (
        "panic: runtime error: invalid memory address" in output
        and "(*Client).GetServices(0x0" in output
        and "(*serviceNode).publishServiceAddresses.func1()" in output
    )
    print("PROVEN closed-node-publication: regression assertion failed")
finally:
    source.write_bytes(baseline)
assert subprocess.check_output(["git", "status", "--porcelain"], text=True) == status
assert source.read_bytes() == baseline
subprocess.run(command, check=True)
print("RESTORED closed-node-publication: source and passing regression")

command = ["go", "test", "-race", "-run", "^TestServiceListenerDefersTailscaleRegistration$", "./..."]
subprocess.run(command, check=True)
needle = b'listenerKey := fmt.Sprintf("%s:%s:%d:%d", serviceNodeName, serviceName, effectivePort, proxyVersion)'
assert baseline.count(needle) == 1
try:
    mutation = b'if _, err := nodes.service.ListenService(serviceName, tsnet.ServiceModeTCP{Port: uint16(effectivePort)}); err != nil { return nil, err }\n\t' + needle
    source.write_bytes(baseline.replace(needle, mutation))
    result = subprocess.run(command, text=True, capture_output=True)
    output = result.stdout + result.stderr
    print(output)
    assert result.returncode != 0
    assert "getServiceListener returned an error" in output
    print("PROVEN deferred-registration: eager startup assertion failed")
finally:
    source.write_bytes(baseline)
assert subprocess.check_output(["git", "status", "--porcelain"], text=True) == status
assert source.read_bytes() == baseline
subprocess.run(command, check=True)
print("RESTORED deferred-registration: source and passing regression")
