import os
from subprocess import Popen, PIPE

def clone_git_repo(repo, destination):
    """
    Clone a git repository
    """
    cmd = "git clone %s %s" % (repo, destination)
    print cmd

    # Popen(cmd, shell=True)
    
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)        
    print "stdout >> " + proc.stdout.read()
    print "stderr >> " + proc.stderr.read()
        
def write_docker_file(stage, install_cmds, add_cmds, default_cmd, mode):
    # Make sure the Dockerfile template exists.
    docker_template = "config/%s/Dockerfile.template" % mode        
    if not os.path.isfile(docker_template):
        print "could not find %s" % docker_template
        return None
    
    docker_file = ""
    with open(docker_template, "r") as f:
        docker_file = f.read()

        docker_file = docker_file.replace("__INSTALL_PACKAGE_STEP__", "\n".join(install_cmds))
        docker_file = docker_file.replace("__INSTALL_SCRIPT_STEP__", "\n".join(add_cmds))
        docker_file = docker_file.replace("__DEFAULT_CMD_STEP__", default_cmd)

    print "Creating Dockerfile"
    docker_file_name = "Dockerfile_stage_%s" % stage        
    with open("pipeline/" + docker_file_name, "w") as f:
        f.write(docker_file)

    return docker_file_name
    
def _compile_docker_file(docker_file, image_name):
    """
    Compile the Dockerfile. 
    """        
    print "compiling dockerfile"
    cmd = "docker build -f %s -t %s ." % (docker_file, image_name)
    print "cmd >> " + cmd
    
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    
    print "stdout >> " + proc.stdout.read()
    print "stderr >> " + proc.stderr.read()        
