# Copyright 2015 Cirruspath, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import koopa
from koopa.generator.docker import DockerGenerator
from koopa.generator.luigigenerator import LuigiGenerator
from koopa.pipeline.luigi import Luigi
from koopa.client.options import CmdHelp
import logging
import logging.config
import os
import sys

#
# Global vars.
#

def compile_and_execute(drakefile=None):
    """
    Call the Drake compiler and execute the Luigi pipeline.
    """

    # Read in the configuration.
    config_file = os.getenv("KOOPA_CONFIG", "config") + "/koopa.json"
    if not os.path.isfile(config_file):
        print "could not find Koopa config"
        sys.exit(1)
    config = json.load(open(config_file))

    # Look in the local directory for the Drakefile if the user hasn't explicitly passed one.
    if not drakefile:
        drakefile = os.getcwd() + "/Drakefile"

    # Check which backend we are targetting.
    if config["BACKEND"] == "DOCKER":
        docker = DockerGenerator()
        pipeline = docker.compile(drakefile)
    else:
        luigi_gen = LuigiGenerator()
        luigi = Luigi()
        pipeline = luigi_gen.compile(drakefile)
        luigi.execute(pipeline=pipeline,
                      server="local")

class CLI(object):
    def __init__(self):
        self.cmds = CmdHelp()
        self.cmds.description = "Simple Luigi Pipeline Builder"
        self.cmds.version = koopa.__version__
        self.cmds.usage = "koopa COMMAND [arg...]"
        self.cmds.add_option("-w", "--workdir", "Specify Drakefile working directory")
        self.cmds.add_option("-h", "--help", "Display help")

def main(argv=None):
    cli = CLI()
    if(sys.argv):
        cli.cmds.parse_args(sys.argv)

        # Initialize the cli
        options = cli.cmds.get_options()
        if '-w' in options:
            compile_and_execute(drakefile=options['-w'][0])
        elif '-h' in options:
            print cli.cmds.print_help()
        else:
            compile_and_execute()
