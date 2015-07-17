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

class Drake(object):
    parser = DrakeParser()

    def compile(self, workdir):
        """
        Convert the Drakefile into a set of Luigi scripts. 

        Keyword arguments:
        workdir -- full path of the working directory. 

        Returns the full paths of the Luigi scripts.
        """
        with open(workdir) as f:
            ast = self.parser.generate_ast(f.read())
            f.close()

        return None
