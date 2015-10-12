"""
Python generator.
"""

import os
import shutil

class PythonGenerator(object):


    def gen_install(self, parent_dir, req_file):
        """
        Generate the installation process for the Python script.
        """

        # Get the full path of the requirements file. 
        requirements = os.path.abspath(os.path.join(parent_dir, req_file))
        
        # Copy the file to the pipeline dir.
        shutil.copyfile(requirements, "pipeline/" + req_file)
        
        # Add from the pipeline dir.        
        return [ 'ADD ./%s /scripts/' % req_file,
                 'RUN pip install -r /scripts/%s' % req_file]

    def gen_script(self, stage, script, parent_dir):
        """
        Generate the actual script that will be executed. 
        """
        
        python_script = ["#! /usr/bin/env python"]
        for s in script:
            python_script.append(s)
            
        # Write out the commands to a temporary file.
        run_cmds = "\n".join(python_script)        
        execute_file_name = "koopa_execute_file_%s" % stage
        with open("pipeline/" + execute_file_name, "w") as f:
            f.write(run_cmds)
            
        return execute_file_name
