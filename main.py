#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_zoho_creator import SourceZohoCreator

if __name__ == "__main__":
    source = SourceZohoCreator()
    launch(source, sys.argv[1:])
