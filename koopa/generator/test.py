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
        pipeline = []        
        parent_dir = os.path.dirname(os.path.abspath(drakefile))
        working_dir = parent_dir.split("/")[-1]
        with open(drakefile) as f:
            stage = 0
            ast = self.parser.generate_ast(f.read())
            
            for k in ast.pipeline.keys():
                if not 'script' in ast.pipeline[k]['options']:
                    mode = "bash"
                else:
                    mode = ast.pipeline[k]['options']['script']
                
                pipeline.append( {'stage': ast.pipeline[k]['script'],
                                  'script': mode,
                                  'dir': parent_dir, 
                                  'io': k,
                                  'header': ast.workflow_options} )
        return pipeline
