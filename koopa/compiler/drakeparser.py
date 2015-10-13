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

    def get_io_substitution(self, keyword, io_list, content):
        new_content = []
        for c in content:
            replacements = {}
            matches = re.findall(keyword, c)
            for match in matches:                
                char = match[-1]
                if char == 'N':
                    input_str = str(len(io_list))
                elif char == 'S':
                    input_str = ' '.join(io_list)
                elif char.isdigit():
                    input_str = inputs[int(io_list)]
                else:
                    input_str = io_list[0]

                replacements[match.strip()] = input_str

            for r in replacements.keys():
                c = c.replace(r, replacements[r])
            new_content.append(c)

        return new_content

    def replace_io_keywords(self, content, inputs, outputs, script_type=None):
        """
        Replaces I/O keywords (INPUT, OUTPUT) in the script with variable values.
        """
        
        if script_type == None or script_type == "bash":
            # Shell scripts use the simple $INPUT[n], $OUTPUT[n] style.
            keywords = ["(\$INPUT.?)", "(\$OUTPUT.?)"]
        else:
            # Otherwise, we have to use square brackets.
            keywords = ["(\$\[INPUT.?\])", "(\$\[OUTPUT.?\])"]
                    
        # Make the keyword substitutions
        content = self.get_io_substitution(keywords[0], inputs, content)        
        content = self.get_io_substitution(keywords[1], outputs, content)
        
        return content

    def is_segment_header(self, line):
        """
        Indicate whether the line is a segment header.
        """

        # Just assume that anything that not a body or comment is part of a new segment.
        return not self.is_segment_body(line) and line[0] != ';'

    def is_segment_body(self, line):
        """
        Indicate whether the line is a segment body line.
        """
        return line[0] == ' ' or line[0] == '\t'

    def parse_job_options(self, option_values):
        options = {}
        for v in option_values:
            o = v[1:-1].split(":")
            if len(o) == 1:
                options["script"] = o[0]
            else:
                options[o[0]] = o[1]
        return options

    def parse_segment_header(self, line):
        """
        Parse the inputs, outputs, and options.
        """

        # First capture the inputs.
        io_split = line.split("<-")

        outputs = io_split[0].split(",")
        outputs = [i.strip() for i in outputs]

        # Before we can capture the outputs, we can need to capture the options.
        # Options have [] brackets around them and only appear after the outputs.
        s = io_split[1].find('[')
        if s > -1:
            options = self.parse_job_options(io_split[1][s:].split(" "))
            inputs = io_split[1][:s].split(",")
        else:
            options = {}
            inputs = io_split[1].split(",")

        inputs = [i.strip() for i in inputs]
        return inputs, outputs, options

    def _count_leading_spaces(self, line):
        """
        Count the number of leading white space in the line. 
        """
        num = 0
        for i in range(len(line)):
            if line[i] == ' ' or line[i] == '\t':
                num += 1
            else:
                break
        return num
        
    def generate_ast(self, drake_content):
        """
        Produce an abstract-syntax-tree for the Drakefile content.
        """
        ast = PipelineAST()
        current_stage = None
        body_line_spaces = None        
        lines = drake_content.split('\n')
        for line in lines:
            # Skip blank lines.
            if line.strip() == "":
                continue

            if self.is_segment_header(line):
                # We are parsing a new pipeline stage. Check if we need to add a prior
                # stage, and then proceed to parsing the actual stage information.

                if current_stage:
                    # Before adding the stage, replace all the I/O keywords.
                    script_type = current_stage['body']['options'].get('script', None)                    
                    current_stage['body']['script'] = self.replace_io_keywords(current_stage['body']['script'],
                                                                               current_stage['io'].input_files,
                                                                               current_stage['io'].output_files,
                                                                               script_type)
                    ast.add_pipeline_step(current_stage['io'], current_stage['body'])

                input_files, output_files, job_options = self.parse_segment_header(line)
                current_stage = { 'io': InputOutputLists(input_files, output_files),
                                  'body': {
                                    'script': [],
                                    'options': job_options
                                  }
                                }

                # Reset the number of leading whitespaces.
                body_line_spaces = None

            elif self.is_segment_body(line):
                # print "parsing body segment"
                # print line

                # How many leading spaces are there in the line? 
                if not body_line_spaces:
                    body_line_spaces = self._count_leading_spaces(line)

                # For each line strip out the leading white spaces. 
                line = line[body_line_spaces:]
                
                # We are parsing the body.
                if current_stage:
                    current_stage['body']['script'].append(line)
                else:
                    # This is an error. We cannot parse a body that is separate from a stage!
                    print "Error!"

        if current_stage:
            # Before adding the stage, replace all the I/O keywords.
            script_type = current_stage['body']['options'].get('script', None)
            current_stage['body']['script'] = self.replace_io_keywords(current_stage['body']['script'],
                                                                       current_stage['io'].input_files,
                                                                       current_stage['io'].output_files,
                                                                       script_type)            
            ast.add_pipeline_step(current_stage['io'], current_stage['body'])

        return ast
