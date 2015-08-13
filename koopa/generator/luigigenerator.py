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
import logging
import logging.config
from subprocess import call
from collections import OrderedDict

class LuigiGenerator(object):
    parser = DrakeParser()

    def compile(self, workdir):
        """
        Convert the Drakefile into a set of Luigi scripts. 

        Keyword arguments:
        workdir -- full path of the working directory. 

        Returns the full paths of the Luigi scripts.
        """
        
        # Write luigi file
        luigi_path = workdir.replace('/Drakefile', '/')
        luigi_filename = luigi_path +'luigi_script.py'
        with open(luigi_filename, 'w') as luigi_file:
            tab = ' '*4
            
            # Write header content
            luigi_file.write('import luigi\n')
            luigi_file.write('from subprocess import call\n\n')
            
            # Write luigi tasks based on AST
            with open(workdir) as f:
                ast = self.parser.generate_ast(f.read())
                dep_graph = DependencyGraph(ast)
                outputs = dep_graph.get_outputs()
                
                for i, output in enumerate(outputs):
                    isLeafNode = True
                    for io_lists in ast.pipeline:
                    
                        # Write task for non-leaf node 
                        if output in io_lists.output_files:
                            isLeafNode = False
                            drake_script = ast.pipeline[io_lists]
                
                            # Debug code
                            # print 'Output files: '+ str(io_lists.output_files)
                            # print 'Task' + str(i)
                            # print 'Input files: '+ str(io_lists.input_files)
                            # for input in io_lists.input_files:
                            #     if input in outputs:
                            #         print 'requires Task' +str(outputs.index(input))
                            #     else:
                            #         print 'requires new Task'
                            # print 'Script type: '+ str(drake_script.script_type)
                            # print 'Script options: '+ str(drake_script.options)
                            # print 'Script content: '+ str(drake_script.content)
                            # print
                
                            # Write requires() function
                            luigi_file.write('class Task{}(luigi.Task):\n'.format(str(i)))
                            luigi_file.write('{}def requires(self): return ['.format(tab))
                            for j, input in enumerate(io_lists.input_files):
                                if j > 0:
                                    luigi_file.write(', ')
                                luigi_file.write('Task{}()'.format(str(outputs.index(input))))
                            luigi_file.write(']\n')
                
                            # Write run() function
                            temp_file = luigi_path + "temp"
                            luigi_file.write('{}def run(self): '.format(tab))
                            if drake_script.script_type == 'shell':
                                luigi_file.write("call('{}', shell=True)\n".format(drake_script.content))
                            elif drake_script.script_type == 'python':
                                luigi_file.write('\n')
                                luigi_file.write("{}call(\"echo '{}' > '{}.py'\", shell=True)\n".format(tab*2, drake_script.content.strip(), temp_file))
                                luigi_file.write("{}call(\"python '{}.py'; rm '{}.py'\", shell=True)\n".format(tab*2, temp_file, temp_file))
                            else:
                                luigi_file.write('\n')
                    
                            # Write output() function
                            luigi_file.write('{}def output(self): return ['.format(tab))
                            for j, output in enumerate(io_lists.output_files):
                                if j > 0:
                                    luigi_file.write(', ')
                                luigi_file.write('luigi.LocalTarget("{}")'.format(output))
                            luigi_file.write(']\n\n')
                            
                            del ast.pipeline[io_lists]
                            
                    # Write task for leaf node
                    if isLeafNode:
                    
                        # Debug code
                        # print 'Output files: '+ output
                        # print 'Task' + str(i)
                        # print
                        
                        luigi_file.write('class Task{}(luigi.Task):\n'.format(str(i)))
                        luigi_file.write('{}def output(self): return luigi.LocalTarget("{}")\n\n'.format(tab, output))
                
                f.close()
                
            # Write footer content
            luigi_file.write('if __name__ == "__main__":\n')
            luigi_file.write('{}luigi.run()'.format(tab))
            luigi_file.close()
            
            # Write luigi execution script
            run_script = luigi_path + 'run_luigi.sh'
            with open(run_script, 'w') as f:
                f.write('luigid > /dev/null 2>&1 &\n')
                f.write('sleep 1\n')
                f.write('python "{}" Task0'.format(luigi_filename))
                f.close()
            call('chmod u+x "{}"'.format(run_script), shell=True)
        
        return luigi_path