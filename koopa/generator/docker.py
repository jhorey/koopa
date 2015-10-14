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
from koopa.compiler.drakeparser import DrakeParser
from koopa.generator.dependencygraph import DependencyGraph
from koopa.generator.dockerutils import *
from koopa.generator.python import PythonGenerator
from koopa.generator.rlang import RGenerator
from koopa.generator.bash import BashGenerator
import logging
import logging.config
import os
from subprocess import Popen, PIPE
import shutil

class SingleDockerGenerator(object):
    parser = DrakeParser()

    def create_drakefile(self, pipeline):        
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

        # First thing to do is to clone the repository.
        package_name = ast.workflow_options['package'].split("/")[-1].split(".")[0]         
        clone_git_repo(ast.workflow_options['package'], "pipeline/%s/" % package_name)
        
        # Now we're going to add the source code to the Docker image. 
        install_cmds = [ "ADD ./pipeline/%s /scripts/" % package_name,
                         "WORKDIR /scripts/%s" % package_name,
                         "RUN %s" % ast.workflow_options['setup'] ]

        # Copy over any necessary output files to the same place.
        copy_cmds = []
        for f in os.listdir(parent_dir):
            if f != "Drakefile":
                shutil.copyfile(os.path.join(parent_dir, f),
                                os.path.join("pipeline", f))
                copy_cmds.append( "ADD ./pipeline/%s /scripts/" % f)

        # Create our sanitized Drakefile. 
        self.create_drakefile(pipeline)
        copy_cmds.append( "ADD ./pipeline/Drakefile /scripts/")
        
        # The actual execution script will be running Drake.
        default_cmd = "CMD [\"drake -a --base /scripts -w /scripts/Drakefile\"]"

        # Write out the actual dockerfile. 
        docker_file = write_docker_file(0, install_cmds, copy_cmds, default_cmd, "python")

        # Compile the Dockerfile.
        # image_name = "cirrus/%s_stage_%d" % (working_dir, stage)
        # compile_docker_file("pipeline/" + docker_file, image_name)
            
        return pipeline
    
class MultiDockerGenerator(object):
    parser = DrakeParser()

    def compile(self, drakefile):
        pipeline = []        
        parent_dir = os.path.dirname(os.path.abspath(drakefile))
        working_dir = parent_dir.split("/")[-1]
        with open(drakefile) as f:
            # Parse the AST and get the dependency graph.
            stage = 0
            ast = self.parser.generate_ast(f.read())
            for k in ast.pipeline.keys():
                # Choose which Dockerfile template to use given the script type.
                # These templates specify how to install packages and how to wrap the
                # commands into the appropriate files.
                if not 'script' in ast.pipeline[k]['options']:
                    mode = "bash"
                else:
                    mode = ast.pipeline[k]['options']['script']
                
                if mode == 'python':
                    # This is a Python script.
                    backend = PythonGenerator()
                elif mode == 'R':
                    # This is an R script.
                    backend = RGenerator()
                elif mode == 'bash':
                    # This is a bash script.
                    backend = BashGenerator()
                else:
                    # Unsupported mode.
                    print "Unsupported script type " + mode
                    return None
            
                # Generate the installation procedure.
                install_cmds = []
                if 'requirements' in ast.pipeline[k]['options']:
                    install_cmds = backend.gen_install(parent_dir, ast.pipeline[k]['options']['requirements'])
            
                # Create the script that will actually execute in the container.
                file_name = backend.gen_script(stage, ast.pipeline[k]['script'], parent_dir)
            
                # Insert into the Docker template and save.
                stage += 1
                add_script_cmd = ["ADD ./pipeline/%s /scripts/" % file_name]
                default_cmd = "CMD [\"/scripts/%s\"]" % file_name
                docker_file = write_docker_file(stage, install_cmds, add_script_cmd, default_cmd, mode)

                # Compile the Dockerfile.
                image_name = "cirrus/%s_stage_%d" % (working_dir, stage)
                pipeline.append( {'image': image_name,
                                 'io': k} )
                compile_docker_file("pipeline/" + docker_file, image_name)
        return pipeline
