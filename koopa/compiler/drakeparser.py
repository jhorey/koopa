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

from koopa.compiler.ast import *
import re

class DrakeParser(object):

    def generate_ast(self, drake_content):
        """
        Produce an abstract-syntax-tree for the Drakefile content. 
        
        Keyword arguments:
        drake_content -- string content of the entire Drakefile.
        
        Returns an abstract-syntax-tree. 
        """
        
        def add_AST_script(ast, outputs, inputs, options, commands):
            """
            Formats arguments as input for add_pipeline_step() in ast.py
            
            Keyword arguments:
            ast: abstract-syntax-tree
            outputs: list of output files & tag dependencies
            inputs: list of input files & tag dependencies
            options: list of Drakefile protocol & misc. options
            commands: list of commands for given protocol
            
            Returns ast.
            """
            
            def replace_io_keywords(content, inputs, outputs, script_type='shell'):
                """
                Replaces I/O keywords in content with variable values.
                
                Keyword arguments:
                content: string of commands for given protocol
                inputs: list of input variables of type string
                outputs: list of output variables of type string
                script_type: script type contained in a string
                
                Returns string of new content.
                """
                
                keywords = ['INPUT', 'OUTPUT']
                var_groups = [inputs, outputs]
                for keyword, vars in zip(keywords, var_groups):
                    
                    # First substitutions
                    if script_type == 'shell':
                        matches = set(re.findall('\$'+keyword+'.', content))
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
                                replstr = vars[0] +' '
                            content = content.replace('$'+keyword+char, str(replstr))
                        content = content.replace('$'+keyword, vars[0])
                    elif script_type == 'python' or script_type == 'R':
                        matches = set(re.findall('\$\['+keyword+'.\]', content))
                        for match in matches:
                            char = match[len(match)-2:len(match)-1]
                            replstr = ''
                            if char == 'N':
                                replstr = len(vars)
                            elif char == 'S':
                                replstr = ' '.join(vars)
                            elif char.isdigit():
                                replstr = vars[int(char)]
                            elif char == ' ':
                                replstr = vars[0] +' '
                            content = content.replace('$['+keyword+char+']', str(replstr))
                        content = content.replace('$['+keyword+']', vars[0])
                        
                    # Second substitutions
                    keyword = keyword.lower()
                    if script_type == 'python':
                        matches = set(re.findall(keyword+'s\[\d\]', content))
                        for match in matches:
                            char = match[len(match)-2:len(match)-1]
                            content = content.replace(keyword+'s['+char+']', vars[int(char)])
                        content = content.replace(keyword+'s.length', str(len(vars)))
                        for end in [' ', '\n']:
                            content = content.replace(keyword+'s'+end, ' '.join(vars)+end)
                            content = content.replace(keyword+end, vars[0]+end)
                    elif script_type == 'R':
                        matches = set(re.findall(keyword+'s\[\d\]', content))
                        for match in matches:
                            char = match[len(match)-2:len(match)-1]
                            content = content.replace(keyword+'s['+char+']', vars[int(char)])
                        content = content.replace('length('+keyword+'s)', str(len(vars)))
                        for end in [' ', '\n']:
                            content = content.replace(keyword+'s'+end, ' '.join(vars)+end)
                            content = content.replace(keyword+end, vars[0]+end)
                    
                return content
            
            # Parse script type
            # Some scripts have script-specific options
            script_type = 'shell'
            script_types = set(['shell', 'ruby', 'python', 'eval', 'clojure', 'lein', 'cascalog', 'R', 'get'])
            script_file_types = set(['ruby-script', 'python-script', 'jar', 'R-script'])
            for option in options:
                option = re.sub('protocol:', '', option)
                if option in script_types:
                    script_type = option
                elif option in script_file_types:
                    print 'Parse script_file_type'
                    
            # Format commands based on script type
            if script_type == 'shell':
                content = '\\n'.join(commands)
                content = content.replace("'", "\\'")
                content = content.replace('"', '\\"')
            elif script_type == 'python' or script_type == 'R':
                content = '\\n'.join(commands)
                content = content.replace("'", "\\'")
                content = content.replace('"', '\\"')
            else:
                content = '\n'.join(commands)
                content = content.replace("'", "\\'")
                
            # Replace input/output keywords in commands with variable values
            content = replace_io_keywords(content, inputs, outputs, script_type)
            
            # Ignore tags in inputs and outputs
            for output, input in zip(outputs, inputs):
                if output != '' and output[0] == '%':
                    outputs.remove(output)
                if input != '' and input[0] == '%':
                    inputs.remove(input)
            
            # Debug code
            # print 'Script type: {}'.format(script_type)
            # print 'Outputs: {}'.format(', '.join(outputs))
            # print 'Inputs: {}'.format(', '.join(inputs))
            # print 'Options: {}'.format(', '.join(options))
            # print 'Commands: {}'.format(commands)
            
            # Format inputs, outputs, options, content for add_pipeline_step()
            io_lists = InputOutputLists(input_files=inputs, output_files=outputs)
            drake_script = DrakeScript(script_type, options, content)
            ast.add_pipeline_step(io_lists, drake_script)
            return ast
        
        # Parse drake_content
        ast = PipelineAST()
        lines = drake_content.split('\n')
        seen_script = False
        for line in lines:
            if line != '' and line[0] != ';':
                
                # I/O and options line
                if line[0] != ' ':
                    if seen_script:
                        # Add previous script to AST
                        ast = add_AST_script(ast, outputs,inputs, options, commands)
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
        ast = add_AST_script(ast, outputs,inputs, options, commands)
        
        return ast
