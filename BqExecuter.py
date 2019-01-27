import subprocess


class BqExecuter(object):


    def execute(self, query):
        try:
            command = ['bq', 'query', '--use_legacy_sql=false', '--dry_run', query]

            output = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE).communicate()[0]
            size_bytes = int(output.split()[14]) # replace with output parser
            return size_bytes

        except (OSError, ValueError, TypeError, IndexError) as e:
            raise Exception('Failed to execute command. reason: {0}'.format(e.message))