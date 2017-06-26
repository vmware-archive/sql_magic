# -*- coding: utf-8 -*-
# Copyright (C) 2017-Present Pivotal Software, Inc. All rights reserved.
#
# This program and the accompanying materials are made available under
# the terms of the Apache License, Version 2.0 (the "License");
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
"""
utils.py
~~~~~~~~~~~~~~~~~~~~~

Utility functions.
"""

import argparse

import sqlparse

from IPython.core.display import display_javascript

try:
    from traitlets import TraitError
except ImportError:
    from IPython.utils.traitlets import TraitError

def add_syntax_coloring():
    """Adds syntax coloring to cell magic for SELECT, FROM, etc."""
    js_sql_syntax = '''
    require(['notebook/js/codecell'], function(codecell) {
      // https://github.com/jupyter/notebook/issues/2453
      codecell.CodeCell.options_default.highlight_modes['magic_text/x-sql'] = {'reg':[/^%read_sql/, /.*=\s*%read_sql/,
                                                                                      /^%%read_sql/]};
      Jupyter.notebook.events.one('kernel_ready.Kernel', function(){
          console.log('BBBBB');
          Jupyter.notebook.get_cells().map(function(cell){
              if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
      });
    });
    '''
    display_javascript(js_sql_syntax, raw=True)

def create_flag_parser():
    """Create parser for reading arguments and flags provided by user in cell magic."""
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', '--notify', help='Toggle option for notifying query result', action='store_true')
    ap.add_argument('-a', '--async', help='Run query in seperate thread. Please be cautious when assigning\
                                           result to a variable', action='store_true')
    ap.add_argument('-d', '--display', help='Toggle option for outputing query result', action='store_true')
    ap.add_argument('-c', '--connection', help='Specify connection object for this query (override default\
                                                connection object)', action='store', default=False)
    ap.add_argument('table_name', nargs='?')
    return ap

def parse_read_sql_args(line_string):
    """Parse arguments."""
    ap = create_flag_parser()
    opts = ap.parse_args(line_string.split())
    return {'table_name': opts.table_name, 'display': opts.display, 'notify': opts.notify,
            'async': opts.async, 'force_caller': opts.connection}


def is_empty_statement(s):
    """Check if SQL statement is blank or commented."""
    if not s:
        return True
    p = sqlparse.parse(s)[0]
    t = p.tokens[0]
    is_a_comment = t.ttype is not None and (t.ttype.parent == sqlparse.tokens.Comment)
    if t.ttype and is_a_comment:
        return True
