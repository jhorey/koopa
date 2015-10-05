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
        replacements = {}
        matches = set(re.findall(keyword, content))
        for match in matches:
            char = match[-1]
            if char == 'N':
                input_str = len(io_list)
            elif char == 'S':
                input_str = ' '.join(io_list)
            elif char.isdigit():
                input_str = inputs[int(io_list)]
            elif char == ' ':
                input_str = io_list[0]

            replacements[match] = input_str

        for r in replacements.keys():
            content = content.replace(r, replacements[r])

        return content

    def replace_io_keywords(self, content, inputs, outputs, script_type='shell'):
        """
        Replaces I/O keywords (INPUT, OUTPUT) in the script with variable values.
        """

        if script_type == "shell":
            # Shell scripts use the simple $INPUT[n], $OUTPUT[n] style.
            keywords = ["\$INPUT.", "\$OUTPUT."]
        else:
            # Otherwise, we have to use square brackets.
            keywords = ["\$[INPUT.]", "\$[OUTPUT.]"]

        # Make the keyword substitutions
        content = self.get_io_substitution(keywords[0], inputs, content)
        content = self.get_io_substitution(keywords[1], outputs, content)

        return content

    def is_segment_header(self, line):
        """
        Indicate whether the line is a segment header.
        """
        # Just assume that anything that not a body or comment is part of a new segment.
        return line[0] != '\t' and line[0] != ';'

    def is_segment_body(self, line):
        """
        Indicate whether the line is a segment body line.
        """
        return line[0] == '\t'

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
        inputs = io_split[0].split(",")

        # Before we can capture the outputs, we can need to capture the options.
        # Options have [] brackets around them and only appear after the outputs.
        s = io_split[1].find('[')
        if s > -1:
            options = self.parse_job_options(io_split[1][s:].split(" "))
            outputs = io_split[1][:s].split(",")
        else:
            options = {}
            outputs = io_split[1].split(",")

        return inputs, outputs, options

    def generate_ast(self, drake_content):
        """
        Produce an abstract-syntax-tree for the Drakefile content.
        """
        ast = PipelineAST()
        current_stage = None
        lines = drake_content.split('\n')
        for line in lines:
            if self.is_segment_header(line):
                # We are parsing a new pipeline stage. Check if we need to add a prior
                # stage, and then proceed to parsing the actual stage information.

                if current_stage:
                    ast.add_pipeline_step(current_stage['io'], current_stage['script'])

                input_files, output_files, job_options = self.parse_segment_header(line)
                current_stage = { 'io': InputOutputLists(input_files, output_files),
                                  'body': {
                                    'script': [],
                                    'options': job_options
                                  }
                                }

            elif self.is_segment_body(line):
                # We are parsing the body.
                if current_stage:
                    current_stage['body']['script'].append(line)
                else:
                    # This is an error. We cannot parse a body that is separate from a stage!
                    print "Error!"

        return ast
