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

from koopa.compiler.drakeparser import DrakeParser
from koopa.generator.dependencygraph import DependencyGraph
import logging
import logging.config
from subprocess import call
from collections import OrderedDict

class DockerGenerator(object):
    parser = DrakeParser()

    def compile(self, drakefile):
        """
        Take the Drakefile and create a set of Docker images for each pipeline stage. Then generate a Luigi job that can process this Docker-based pipeline.
        """

        print "Using Drakefile " + drakefile
        with open(drakefile) as f:
            # Parse the AST and get the dependency graph.
            ast = self.parser.generate_ast(f.read())
            for k in ast.pipeline.keys():
                print k.input_files
                print k.output_files

                print ast.pipeline[k].script_type
                print ast.pipeline[k].options
                print ast.pipeline[k].content

                # Choose which Dockerfile template to use given the script type.
                # These templates specify how to install packages and how to wrap the
                # commands into the appropriate files. 
