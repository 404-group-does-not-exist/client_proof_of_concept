#!/usr/bin/env python3
from wifiology_node_poc.procedures import upload_argument_parser, upload_argparse_args_to_kwargs, run_upload

if __name__ == "__main__":
    run_upload(
        **upload_argparse_args_to_kwargs(upload_argument_parser.parse_args())
    )
