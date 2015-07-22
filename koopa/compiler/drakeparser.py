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
import re

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
            Formats arguments as input for add_pipeline_step() in ast.py
            
            Keyword arguments:
            outputs: list of output files & tag dependencies
            inputs: list of input files & tag dependencies
            options: list of Drakefile protocol & misc. options
            commands: list of commands for given protocol
            
            Returns nothing.
            """
            
            def replace_io_keywords(commands, are_outputs, vars):
                """
                Replaces I/O keywords in commands with variable values.
                
                Keyword arguments:
                commands: list of commands for given protocol
                are_outputs: boolean designating vars as inputs or outputs. 
                         False for inputs and True for outputs.
                vars: list of input or output variables of type string
                
                Returns list of new commands.
                """
                
                if are_outputs:
                    keyword = 'OUTPUT'
                else:
                    keyword = 'INPUT'
                for i, command in enumerate(commands):
                    matches = set(re.findall('\$'+keyword+'.', command))
                    for match in matches:
                        char = match[len(match)-1:]
                        replstr = ''
                        if char == 'N':
                            replstr = len(vars)
                        elif char == 'S':
                            replstr = ' '.join(vars)
                        elif char.isdigit():
                            replstr = vars[int(char)]
                        elif char == ' ':
                            replstr = vars[0]
                        command = command.replace('$'+keyword+char, str(replstr))
                    commands[i] = command
                return commands
            
            # Replace input/output keywords in commands with variable values
            commands = replace_io_keywords(commands=commands, are_outputs=False, vars=inputs)
            commands = replace_io_keywords(commands=commands, are_outputs=True, vars=outputs)
            
            # Ignore tags in inputs and outputs
            for output, input in zip(outputs, inputs):
                if output != '' and output[0] == '%':
                    outputs.remove(output)
                if input != '' and input[0] == '%':
                    inputs.remove(input)
            
            # Temporary debug code
            print 'Outputs: {}'.format(', '.join(outputs))
            print 'Inputs: {}'.format(', '.join(inputs))
            print 'Options: {}'.format(', '.join(options))
            print 'Commands:\n{}'.format('\n'.join(commands))
            return None
        
        # Parse drake_content
        lines = drake_content.split('\n')
        seen_script = False
        for line in lines:
            if line != '' and line[0] != ';':
                
                # I/O and options line
                if line[0] != ' ':
                    if seen_script:
                        # Add previous script to AST
                        add_AST_script(outputs,inputs, options, commands)
                    seen_script = True
                    
                    # Parse I/O line
                    options = list()
                    commands = list()
                    
                    parts = line.split('<-')
                    outputs = [part.strip() for part in parts[0].strip().split(',')]
                    parts = parts[1].split('[')
                    inputs = [part.strip() for part in parts[0].strip().split(',')]
                    if len(parts) > 1:
                        parts = parts[1].strip().split()
                        for token in parts:
                            options.append(token.strip('] '))
                
                # Script command
                else:
                    commands.append(line)
                
        # Add last script to AST
        add_AST_script(outputs,inputs, options, commands)
        
        return None
