#!/usr/bin/env python3
from wifiology_node_poc.procedures import janitor_argument_parser, janitor_argparse_args_to_kwargs, run_janitor

if __name__ == "__main__":
    run_janitor(**janitor_argparse_args_to_kwargs(janitor_argument_parser.parse_args()))
