"""
Python generator.
"""

import os

class PythonGenerator(object):


    def gen_install(self, requirements):
        """
        Generate the installation process for the Python script.
        """
        return [ 'pip install -r %s' % requirements]


    def gen_script(self, stage, script):
        """
        Generate the actual script that will be executed. 
        """
        
        python_script = ["#! /usr/bin/env python"]
        for s in script:
            python_script.append(s)
            run_cmds = "\n".join(python_script)

        # Write out the commands to a temporary file. 
        execute_file_name = "koopa_execute_file_%s" % stage
        with open("pipeline/" + execute_file_name, "w") as f:
            f.write(run_cmds)
            
        return execute_file_name
