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
import os
import shutil
from subprocess import Popen, PIPE

class Test(object):
    
    def generate_plan(self, pipeline):
        """
        The Test engine pipeline just takes the Drakefile and strips all
        the Koopa-specific options. 
        """
        
        workdir = "pipeline"
        if len(pipeline) > 0 and 'workdir' in pipeline[0]['header']:
            workdir = pipeline[0]['header']['workdir']

        # Check if we need to create the working directory.
        if not os.path.isdir(workdir):
            os.mkdir(workdir)
            
        stripped_file = open(os.path.join(workdir, 'Drakefile'), "w")
        for stage in pipeline:

            output_files = ",".join(stage['io'].output_files)
            input_files = ",".join(stage['io'].input_files)
            
            if stage['script'] == "bash":
                option = ""
            else:
                option = "[%s]" % stage['script']
                
            stripped_file.write("%s <- %s %s\n" % (output_files, input_files, option))
            for s in stage['stage']:
                stripped_file.write("    %s\n" % s)

            stripped_file.write("\n")

        stripped_file.close()
        return { 'drake': os.path.join(workdir, 'Drakefile'),
                 'workdir': workdir,
                 'base': stage['dir'] }
    
    def execute(self, pipeline=None, server="local"):
        """
        For the Test engine, the only thing we need is the original Drakefile. 
        """

        # Copy over any necessary output files to the same place as our Drakefile.
        # Otherwise Drake will throw a fit. 
        drake_dir = os.path.dirname(pipeline['drake'])
        input_dir = pipeline['base']
        for f in os.listdir(input_dir):
            if f != "Drakefile":
                shutil.copyfile(os.path.join(input_dir, f),
                                os.path.join(drake_dir, f))
        
        cmd = "drake -a --base %s -w %s" % (os.path.abspath(pipeline['workdir']), pipeline['drake'])
        print cmd
        
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        
        print "stdout >> " + proc.stdout.read()
        print "stderr >> " + proc.stderr.read()                

        return None
