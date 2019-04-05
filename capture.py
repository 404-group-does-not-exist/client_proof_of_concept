#!/usr/bin/env python3
from wifiology_node_poc.procedures import capture_argument_parser, capture_argparse_args_to_kwargs, run_capture

if __name__ == "__main__":
    run_capture(
        **capture_argparse_args_to_kwargs(capture_argument_parser.parse_args())
    )
