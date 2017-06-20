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

import time

from IPython.core.display import HTML


class Notify(object):

    def __init__(self, shell):
        self.shell = shell

    def notify_complete(self, del_time, return_name, return_shape):
        pretty_time = time.strftime('%I:%M:%S %p %Z')
        cell_id = int(time.time())
        cur_time = (1+time.time())*1000.  # fixes issue where browser pops up on reload
        string_args = {
            'pretty_time': pretty_time,
            'del_time': del_time,
            'cell_id': cell_id,
            'return_name': return_name,
            'return_shape': return_shape,
            'cur_time': cur_time
        }
        add_cell_id = '<a id="{cell_id}"></a>\n'.format(cell_id=cell_id)
        alert_str = '''
        <script>
        function notifyMe() {{
          if (Notification.permission !== "granted")
            Notification.requestPermission();
          else {{
            var notification = new Notification('Query Finished in {del_time:2.2f} m', {{
              icon: 'https://raw.githubusercontent.com/crawles/Logos/master/jupyter.png?raw=true',
              body: "{pretty_time}\\n\\nName: {return_name}\\nDimensions: {return_shape}",
            }});

            notification.onclick = function () {{
              document.getElementById('{cell_id}').scrollIntoView();
            }};

          }}
        }}
        // prevents notifications from popping up when notebook is re-opened
        if (Date.now() < {cur_time}) {{
        notifyMe(); }};
        </script>
        '''.format(**string_args)
        html_str = add_cell_id + alert_str
        self.shell.displayhook(HTML(html_str))
