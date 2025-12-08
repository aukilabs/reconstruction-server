#!/usr/bin/env python3
import sys
import time


def main():
    mode = sys.argv[1] if len(sys.argv) >= 2 else "print"
    if mode == "print":
        stdout_msg = sys.argv[2] if len(sys.argv) > 2 else "stdout"
        stderr_msg = sys.argv[3] if len(sys.argv) > 3 else "stderr"
        print(stdout_msg)
        print(stderr_msg, file=sys.stderr)
        return 0
    if mode == "sleep":
        duration = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
        time.sleep(duration)
        return 0
    if mode == "exit":
        code = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        return code

    print(f"unknown mode {mode}", file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())
