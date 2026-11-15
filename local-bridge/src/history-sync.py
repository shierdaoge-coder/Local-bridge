"""Local history synchronization. The plugin supplies the bound tool connection."""

# 在各宿主自己的副本里改成 True，开启自然对话中的同步提示。
AUTO_SYNC_HISTORY = False

import argparse
import json
import sys
import time


class ToolError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def send(value):
    line = json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n"
    sys.stdout.write(line)
    sys.stdout.flush()


def call(name, **arguments):
    send({"tool": name, "arguments": arguments})
    reply = json.loads(sys.stdin.readline())
    if reply.get("ok") is not True:
        raise ToolError(reply.get("error", "TOOL_FAILED"))
    return reply["value"]


TRANSIENT = ("TIMEOUT", "ECONNRESET", "ECONNREFUSED", "EPIPE", "EAI_AGAIN")


def retry(name, **arguments):
    """Retry only temporary failures. The same request and arguments are reused."""
    for delay in (0, 0.12, 0.30):
        time.sleep(delay)
        try:
            return call(name, **arguments)
        except ToolError as error:
            if error.code not in TRANSIENT:
                raise
            last_error = error
    raise last_error


def receive_history(request):
    """确认当前会话，然后领取一批。读完后才进入确认阶段。"""
    endpoint = retry("bridge_context")["endpoint"]
    settings = endpoint["settings"]
    if settings.get("paused") or settings.get("receive_history") is False:
        return {"state": "disabled"}
    arguments = {"archive_id": request["archive_id"], "request_id": request["request_id"]}
    if "budgetBytes" in request:
        arguments["budgetBytes"] = request["budgetBytes"]
    batch = retry("bridge_receive", **arguments)
    return {"state": "read" if batch.get("receipt") else "empty"}


def confirm_history(request):
    """确认结果不明时，核对该批是否已经提交。"""
    arguments = {"receipt": request["receipt"], "request_id": request["request_id"]}
    try:
        retry("bridge_ack", **arguments)
        return {"state": "confirmed"}
    except ToolError as error:
        if error.code not in TRANSIENT + ("CLAIM_EXPIRED", "INVALID_CLAIM", "STALE_CONNECTION"):
            raise
        state = retry(
            "bridge_history_cursor",
            archive_id=request["archive_id"],
            generation=request["generation"],
            request_id=request["request_id"],
        )
        if state.get("batch_state") == "confirmed":
            return {"state": "confirmed"}
        return {"state": "pending", "batch_state": state.get("batch_state", "unknown")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="Check Python and report the automatic trigger switch.")
    args = parser.parse_args()
    if args.probe:
        send({"ok": True, "auto_sync_history": AUTO_SYNC_HISTORY, "protocol": 1})
        return

    request = json.loads(sys.stdin.readline())
    actions = {"receive": receive_history, "confirm": confirm_history}
    try:
        result = actions[request["action"]](request)
        send({"done": result})
    except ToolError as error:
        send({"error": error.code})
    except (KeyError, TypeError, ValueError):
        send({"error": "INVALID_SYNC_REQUEST"})


if __name__ == "__main__":
    main()
