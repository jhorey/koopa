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
import koopa
from koopa.pipeline.drake import Drake
from koopa.pipeline.luigi import Luigi
from koopa.client.options import CmdHelp
import logging
import logging.config
import os
import sys

#
# Global vars. 
# 
drake = Drake()
luigi = Luigi()

def compile_and_execute(workdir=None):
    """
    Call the Drake compiler and execute the Luigi pipeline. 
    """
    
    if not workdir:
        workdir = os.getcwd() + "/Drakefile"

    luigi_pipeline = drake.compile(workdir)
    luigi.execute(pipeline=luigi_pipeline,
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
            compile_and_execute(workdir=options['-w'])
        else:
            compile_and_execute(workdir=None)

