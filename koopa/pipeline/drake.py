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
import logging
import logging.config
from subprocess import call
from collections import OrderedDict
from copy import deepcopy

class Drake(object):
    parser = DrakeParser()

    def compile(self, workdir):
        """
        Convert the Drakefile into a set of Luigi scripts. 

        Keyword arguments:
        workdir -- full path of the working directory. 

        Returns the full paths of the Luigi scripts.
        """
        
        def create_dependency_graph(ast):
            """
            Creates dependency graph from abstract-syntax tree.
            
            Keyword arguments:
            ast -- abstract-syntax tree as an ordered dictionary. 
                Keys are InputOutputLists and values are DrakeScripts.
            
            Returns dependency graph as a dictionary.
            """
            
            def print_graph_bfs(graph):
                """
                Prints the ordered dictionary in BFS order.
            
                Keyword arguments:
                graph -- the dependency graph represented in an ordered dictionary
            
                Returns nothing.
                """
            
                while graph != OrderedDict():
                    key, value = graph.popitem(last=False)
                    print key
                    if isinstance(value, dict):
                        for key2 in value:
                            graph[key2] = value[key2]

            # Create disjoint graphs
            graph = OrderedDict()
            for i, io_lists in enumerate(ast.pipeline):
                for output in io_lists.output_files:
                    graph[output] = OrderedDict()
                    for input in io_lists.input_files:
                        graph[output][input] = OrderedDict()
            
            # Merge disjoint graphs
            keys_set = set()
            for output in graph:
                for input in graph[output]:
                    if input in graph.keys():
                        for key in graph[input]:
                            graph[output][input][key] = graph[input][key]
                            keys_set.add(input)
            for key in keys_set:
                del graph[key]
            print_graph_bfs(deepcopy(graph))
            
            return graph
        
        # Write luigi file
        luigi_path = workdir.replace('/Drakefile', '/')
        luigi_filename = luigi_path +'luigi_script.py'
        with open(luigi_filename, 'w') as luigi_file:
            '''
            # Write header content
            luigi_file.write('import luigi\n')
            luigi_file.write('from subprocess import call\n\n')
            '''
            
            # Write luigi tasks based on AST
            with open(workdir) as f:
                ast = self.parser.generate_ast(f.read())
                graph = create_dependency_graph(ast)
                
                '''
                for i, io_lists in enumerate(ast.pipeline):
                    drake_script = ast.pipeline[io_lists]
                    
                    # Debug code
                    # print 'Input files: '+ str(io_lists.input_files)
                    # print 'Output files: '+ str(io_lists.output_files)
                    # print 'Script type: '+ str(drake_script.script_type)
                    # print 'Script options: '+ str(drake_script.options)
                    # print 'Script content: '+ str(drake_script.content)
                    
                    # Write input task
                    tab = ' '*4
                    luigi_file.write('class InputTask{}(luigi.Task):\n'.format(str(i)))
                    luigi_file.write(tab +'def output(self): return [')
                    for j, input in enumerate(io_lists.input_files):
                        if j > 0:
                            luigi_file.write(', ')
                        luigi_file.write('luigi.LocalTarget("{}")'.format(input))
                    luigi_file.write(']\n\n')
                    
                    # Write output task
                    # Write requires() function
                    luigi_file.write('class OutputTask{}(luigi.Task):\n'.format(str(i)))
                    luigi_file.write('{}def requires(self): return [InputTask{}()'.format(tab, str(i)))
                    if i > 0:
                        luigi_file.write(', OutputTask{}()'.format(str(i-1)))
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
                    
                    last_output_task = i
                '''
                    
                f.close()
                
            '''
            # Write footer content
            luigi_file.write('if __name__ == "__main__":\n')
            luigi_file.write('{}luigi.run()'.format(tab))
            luigi_file.close()
            
            # Write luigi execution script
            run_script = luigi_path + 'run_luigi.sh'
            with open(run_script, 'w') as f:
                f.write('luigid > /dev/null 2>&1 &\n')
                f.write('sleep 1\n')
                f.write('python "{}" OutputTask{}'.format(luigi_filename, last_output_task))
                f.close()
            call('chmod u+x "{}"'.format(run_script), shell=True)
            '''
        
        return luigi_path
