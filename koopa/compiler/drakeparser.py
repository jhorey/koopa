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

from koopa.compiler.ast import PipelineAST

class DrakeParser(object):

    def generate_ast(self, drake_content):
        """
        Produce an abstract-syntax-tree for the Drakefile content. 
        
        Keyword arguments:
        drake_content -- string content of the entire Drakefile.
        
        Returns an abstract-syntax-tree. 
        """
        
        def add_AST_script(outputs, inputs, options, commands):
            """
            Calls add_pipeline_step() in ast.py
            
            Keyword arguments:
            outputs: output files & tag dependencies
            inputs: input files & tag dependencies
            options: Drakefile shell & misc. options
            commands: commands for given shell
            
            Returns nothing.
            """
            
            # Temporary debug code
            print 'Outputs: {}'.format(' '.join(outputs))
            print 'Inputs: {}'.format(' '.join(inputs))
            print 'Options: {}'.format(' '.join(options))
            print 'Commands:\n{}'.format('\n'.join(commands))
            return None
        
        lines = drake_content.split('\n')
        seen_script = False
        for line in lines:
            if line != '' and line[0] != ';':
                
                # I/O line
                if line[0] != ' ':
                    if seen_script:
                        # Add previous script to AST
                        add_AST_script(outputs,inputs, options, commands)
                    seen_script = True
                    
                    # Parse I/O line
                    inputs = list()
                    outputs = list()
                    options = list()
                    commands = list()
                    
                    parts = line.split('<-')
                    outputs = parts[0].split(',')
                    parts = parts[1].strip().split()
                    inputs = parts[0].split(',')
                    for token in parts[1:]:
                        options.append(token.strip('[] '))
                
                # Script Command
                else:
                    commands.append(line)
                
        # Add last script to AST
        add_AST_script(outputs,inputs, options, commands)
        
        return None
