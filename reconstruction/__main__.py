"""Run one pipeline stage: python3 -m reconstruction {local,local-auki-session,global,update,topology} [arguments].

The Rust runners start this as `python3 -m reconstruction <stage> <args>`, with the same
arguments they passed to main.py and update_main.py before. Only the chosen stage is imported.
"""
import argparse
import importlib
import sys

STAGES = {
    "local": "reconstruction.entrypoints.local",
    "local-auki-session": "reconstruction.entrypoints.local_auki_session",
    "global": "reconstruction.entrypoints.global_map",
    "update": "reconstruction.entrypoints.update",
    "topology": "reconstruction.entrypoints.topology",
}


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(prog="python3 -m reconstruction", description=__doc__.splitlines()[0])
    parser.add_argument("stage", choices=STAGES, help="Stage to run; pass --help after it for its arguments")
    stage = parser.parse_args(argv[:1]).stage

    entrypoint = importlib.import_module(STAGES[stage])
    entrypoint.main(entrypoint.parse_args(argv[1:], prog=f"python3 -m reconstruction {stage}"))


if __name__ == "__main__":
    main()
