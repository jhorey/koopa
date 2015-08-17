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

class DependencyGraph:
    """
    Dependency graph class.
    
    Class members:
    graph -- an ordered dictionary that holds the dependency graph
    """
    
    graph = OrderedDict()
    
    def __init__(self, ast):
        """
        DependencyGraph constructor.
        
        Keyword arguments:
        ast -- abstract-syntax tree in an ordered dictionary.
        """
        self.create_dependency_graph(ast)
        return None
    
    def create_dependency_graph(self, ast):
        """
        Creates dependency graph from abstract-syntax tree.
        
        Keyword arguments:
        ast -- abstract-syntax tree in an ordered dictionary. 
            Keys are InputOutputLists and values are DrakeScripts.
        
        Returns nothing.
        """

        # Create disjoint graphs
        for io_lists in ast.pipeline:
            for output in io_lists.output_files:
                self.graph[output] = OrderedDict()
                for input in io_lists.input_files:
                    self.graph[output][input] = OrderedDict()
        
        # Merge disjoint graphs
        keys_set = set()
        for output in self.graph:
            for input in self.graph[output]:
                if input in self.graph:
                    for key in self.graph[input]:
                        self.graph[output][input][key] = self.graph[input][key]
                        keys_set.add(input)
                        
        # Remove redundant dependencies
        for key in keys_set:
            del self.graph[key]
        return None
            
    def get_dependency_graph(self):
        """
        Returns the dependency graph.
        """
        return self.graph
        
    def get_dependencies(self, target, graph, dep_list):
        """
        Recursive function to find dependencies in graph that contains target.
        
        Keyword arguments:
        target -- the key in the dependency of type string
        dep_list -- list of dependencies passed by reference
        
        Returns a list of ordered dictionaries.
        """
        
        if graph == OrderedDict(): return
        if target in graph:
            dep_list.append(graph)
            return dep_list
        for key in graph:
            self.get_dependencies(target, graph[key], dep_list)
        return dep_list
    
    def add_dependency(self, dep):
        """
        Adds one dependency to the dependency graph.
        WARNING: overwrites current existing dependencies.
        
        Keyword arguments:
        dep -- dependency in an ordered dict.
        
        Returns True if successful or False if dep is empty.
        """
        
        if dep == OrderedDict(): return False
        dep_key, dep_dict = dep.popitem()
        graph_list = self.get_dependencies(dep_key, self.graph, list())
        if graph_list != None:
            if graph_list != list():
                for graph in graph_list:
                    graph[dep_key] = dep_dict
            else:
                self.graph[dep_key] = dep_dict
            return True
        return False
            
    def print_graph(self, isDFS):
        """
        Prints the dependency graph in BFS or DFS order.

        Keyword arguments:
        isDFS -- True if printing in DFS order. False if printing in BFS order.

        Returns nothing.
        """
    
        graph2 = deepcopy(self.graph)
        while graph2 != OrderedDict():
            key, value = graph2.popitem(last=isDFS)
            print key
            for key2 in value:
                graph2[key2] = value[key2]
        return None

class Drake(object):
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
            '''
            # Write header content
            luigi_file.write('import luigi\n')
            luigi_file.write('from subprocess import call\n\n')
            '''
            
            # Write luigi tasks based on AST
            with open(workdir) as f:
                ast = self.parser.generate_ast(f.read())
                dep_graph = DependencyGraph(ast)
                
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
