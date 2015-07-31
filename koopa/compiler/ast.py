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

from collections import OrderedDict

class PipelineAST(object):
    """
    The abstract-syntax-tree that logically represents a Drakefile.

    The logical representation for the pipeline:
    [ InputOutputLists ] -> [ DrakeScript ]

    """
    pipeline = OrderedDict()

    def add_pipeline_step(self, io_lists, opcmd_lists):
        """
        Add a new pipeline step. 
        """
        self.pipeline[io_lists] = opcmd_lists

class InputOutputLists:
    """
    Simple lists with names of the output and input files but tags. 
    """
    input_files = list()
    output_files = list()
    
    def __init__(self, input_files, output_files):
        self.input_files = input_files
        self.output_files = output_files
    
class OptionCommandLists:
    """
    Simple lists with Drakefile options and Drake script commands.
    """
    script_type = 'shell'
    options = list()
    script = None
    
    def __init__(self, script_type, options, commands):
        self.options = options
        content = '\n'.join(commands)
        self.script = DrakeScript(script_type, content)
            
class DrakeScript:
    """
    Text of the command. 
    """
    content = None
    script_type = None

    def __init__(self, script_type, content):
        self.script_type = script_type
        self.content = content