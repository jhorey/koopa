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
from koopa.generator.dependencygraph import DependencyGraph
from koopa.generator.python import PythonGenerator
from koopa.generator.rlang import RGenerator
from koopa.generator.bash import BashGenerator
import logging
import logging.config
from subprocess import call
from collections import OrderedDict

class DockerGenerator(object):
    parser = DrakeParser()

    def compile(self, drakefile):
        """
        Take the Drakefile and create a set of Docker images for each pipeline stage. Then generate a Luigi job that can process this Docker-based pipeline.
        """
        
        print "Using Drakefile " + drakefile
        with open(drakefile) as f:
            # Parse the AST and get the dependency graph.
            stage = 0
            ast = self.parser.generate_ast(f.read())
            for k in ast.pipeline.keys():
                # Choose which Dockerfile template to use given the script type.
                # These templates specify how to install packages and how to wrap the
                # commands into the appropriate files.
                print str(ast.pipeline[k]['options'])

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
                    install_cmds = backend.gen_install(ast.pipeline[k]['options']['requirements'])
            
                # Create the script that will actually execute in the container.
                file_name = backend.gen_script(stage, ast.pipeline[k]['script'])
            
                # Insert into the Docker template and save.
                stage += 1
                self._write_docker_file(stage, install_cmds, file_name, mode)

    def _write_docker_file(self, stage, install_cmds, execute_file, mode):
        """
        Create a Dockerfile to run this pipeline stage. 
        """

        print "Reading Dockerfile template"

        # Make sure the Dockerfile template exists.
        docker_template = "config/%s/Dockerfile.template" % mode        
        if not os.path.isfile(docker_template):
            print "could not find %s" % docker_template
            return None
        
        docker_file = ""
        with open(docker_template, "r") as f:
            docker_file = f.read()

            add_script_cmd = "ADD ./%s /scripts/" % execute_file
            default_script_cmd = "CMD [/scripts/%s]" % execute_file            
            
            docker_file = docker_file.replace("__INSTALL_PACKAGE_STEP__", "\n".join(install_cmds))
            docker_file = docker_file.replace("__INSTALL_SCRIPT_STEP__", add_script_cmd)
            docker_file = docker_file.replace("__DEFAULT_CMD_STEP__", default_script_cmd)            

        print "Compiling Dockerfile"
        docker_file_name = "Dockerfile_stage_%s" % stage        
        with open("config/" + docker_file_name, "w") as f:
            f.write(docker_file)
            
