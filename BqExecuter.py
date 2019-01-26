import subprocess


class BqExecuter(object):


    def execute(self, query):
        try:
            #command = ['bq', 'query', '--use_legacy_sql=false', '--dry_run', r'select pv.userPageviewCounters.counterNames from `taboola-data.pageviews.pageviews_20190120`']
            command = ['bq', 'query', '--use_legacy_sql=false', '--dry_run', query]

            output = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE).communicate()[0]
            size_bytes = output.split()[14]
            return size_bytes
            #print size_bytes
            # for line in output.splitlines():
            #     size = line.split('\t')[0]
            #     directory = line.split('\t')[1]
            #     self._directory_size_map[directory] = int(size)
        except (OSError, ValueError, TypeError, IndexError) as e:
            raise Exception('Failed to execute command. reason: {0}'.format(e.message))