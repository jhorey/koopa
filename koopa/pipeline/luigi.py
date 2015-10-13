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

import logging

class Luigi(object):

    def generate_plan(self, pipeline):
        """
        Generate a Luigi-based execution plan. 
        """
        return None
    
    def execute(self, pipeline=None, server="local"):
        """
        Execute the Luigi scripts.

        Keyword arguments:
        pipeline -- full path of the Luigi script.
        server -- path of where to execute the Luigi script. 

        Returns the status message after executing the pipeline. 
        """
        return None
