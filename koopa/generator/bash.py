"""
Bash generator.
"""

class BashGenerator(object):


    def gen_install(self, parent_dir, req_file):
        """
        Generate the installation process for the Python script.
        """

        # Read in the requirements file.
        with open(os.path.join(parent_dir, req_file), 'r') as f:
            requirements = f.read()
            req_string = " ".join(requirements)
            return ['RUN apt-get --yes install ' + req_string]

    def gen_script(self, stage, script, parent_dir):
        """
        Generate the actual script that will be executed. 
        """
        bash_script = ["#! /bin/bash"]
        for s in script:
            bash_script.append(s)

        run_cmds = "\n".join(bash_script)
        execute_file_name = "koopa_execute_file_%s" % stage
        with open("pipeline/" + execute_file_name, "w") as f:
            f.write(run_cmds)
            
        return execute_file_name
