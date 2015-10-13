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
import logging
import os

class TestGenerator(object):
    parser = DrakeParser()

    def compile(self, drakefile):
        """
        Take the Drakefile and create a set of Docker images for each pipeline stage. Then generate a Luigi job that can process this Docker-based pipeline.
        """
        
        print "Using Drakefile " + drakefile

        pipeline = []        
        parent_dir = os.path.dirname(os.path.abspath(drakefile))
        working_dir = parent_dir.split("/")[-1]
        with open(drakefile) as f:
            # Parse the AST and get the dependency graph.
            stage = 0
            ast = self.parser.generate_ast(f.read())
            for k in ast.pipeline.keys():
                pipeline.append( {'stage': ast.pipeline[k]['script'],
                                  'script': ast.pipeline[k]['options']['script'],
                                  'io': k} )
        return pipeline
