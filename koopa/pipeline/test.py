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

import logging
from subprocess import Popen, PIPE

class Test(object):

    def generate_plan(self, pipeline):
        """
        The Test engine pipeline just takes the Drakefile and strips all
        the Koopa-specific options. 
        """

        stripped_file = open("pipeline/Drakefile", "w")
        for stage in pipeline:
            output_files = ",".join(stage['io'].output_files)
            input_files = ",".join(stage['io'].input_files)
            stripped_file.write("%s <- %s [%s]" % (output_files, input_files, stage['script']))
            for s in stage['stage']:
                stripped_file.write("    %s" % s)

        stripped_file.close()        
        return "pipeline/Drakefile"
    
    def execute(self, pipeline=None, server="local"):
        """
        For the Test engine, the only thing we need is the original Drakefile. 
        """
        cmd = "drake -w %s" % pipeline
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        
        print "stdout >> " + proc.stdout.read()
        print "stderr >> " + proc.stderr.read()                

        return None
