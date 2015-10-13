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
from koopa.generator.test import TestGenerator
from koopa.generator.docker import DockerGenerator
from koopa.pipeline.test import Test
from koopa.pipeline.luigi import Luigi
from koopa.pipeline.drake import Drake
from koopa.client.options import CmdHelp
import logging
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

    # How are we going to execute the Drakefile? 
    if config["PIPELINE"] == "TEST":
        # We are going to just execute the Drakefile as-is without
        # compilation, etc. This is just used for testing purposes.
        engine = Test()
    elif config["PIPELINE"] == "DRAKE":
        # We are using the Drake execution engine.
        engine = Drake()
    elif config["PIPELINE"] == "LUIGI":
        # We are using the Luigi execution engine. 
        engine = Luigi()
            
    # Now we need to know how to compile the Drakefile. 
    if config["COMPILE"] == "TEST":
        # Generate a set of Docker images that does the actual execution. 
        generator = TestGenerator()
    elif config["COMPILE"] == "DOCKER":
        # Generate a set of Docker images that does the actual execution. 
        generator = DockerGenerator()
            
    # Pass the pipeline information to the engine so that it can
    # generate the execution plan.
    pipeline = generator.compile(drakefile)    
    plan = engine.generate_plan(pipeline)
    engine.execute(pipeline=plan)
                            
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
