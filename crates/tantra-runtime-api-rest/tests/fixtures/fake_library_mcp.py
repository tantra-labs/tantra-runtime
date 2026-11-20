import json
import os
import sys

mode = sys.argv[1] if len(sys.argv) > 1 else "echo"
marker = sys.argv[2] if len(sys.argv) > 2 else None


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


if mode == "no_init":
    for line in sys.stdin:
        pass
    sys.exit(0)

initialized = False
for line in sys.stdin:
    if mode == "stop_reading" and initialized:
        import time
        time.sleep(60)
    req = json.loads(line)
    method = req.get("method")
    rid = req.get("id")
    if method == "initialize":
        initialized = True
        send({"jsonrpc": "2.0", "id": rid, "result": {"protocolVersion": "2024-11-05", "capabilities": {}}})
    elif method == "ping":
        if mode == "sleep_ping":
            continue
        send({"jsonrpc": "2.0", "id": rid, "result": {}})
    elif method == "tools/call":
        params = req["params"]
        if mode == "sleep":
            continue
        if mode == "crash" and marker and not os.path.exists(marker):
            open(marker, "w").close()
            sys.exit(1)
        if mode == "tool_error":
            body = json.dumps({"error": "invalid_param", "message": "x"})
            send({"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": body}], "isError": True}})
            continue
        if mode == "huge":
            sys.stdout.write("x" * 5000 + "\n")
            sys.stdout.flush()
            continue
        body = json.dumps({"tool": params["name"], "arguments": params["arguments"]})
        send({"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": body}], "isError": False}})
