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
        
    def get_outputs(self, isDFS=False):
        """
        Returns list of outputs in dependency graph.
        
        Keyword arguments:
        isDFS -- True if outputs are in DFS order. False if in BFS order.
        """
        
        outputs = list()
        graph2 = deepcopy(self.graph) 
        while graph2 != OrderedDict():
            key, value = graph2.popitem(last=isDFS)
            outputs.append(key)
            for key2 in value:
                graph2[key2] = value[key2]
        return outputs
            