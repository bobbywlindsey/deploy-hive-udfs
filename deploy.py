import subprocess
import fire
import getpass
from os import listdir
from os.path import isfile, join
import jaydebeapi
from termcolor import colored


def compile_jar_file():
    """
    Compile the Maven project into a single jar file

    :return: bool
    """
    compile_command = ['mvn', 'clean', 'compile', 'assembly:single']
    bytes_output = subprocess.Popen(compile_command, stdout=subprocess.PIPE).communicate()
    utf8_output = bytes_output[0].decode('utf-8')
    if 'BUILD SUCCESS' in utf8_output:
        return True
    return False


def get_jar_file_name():
    """
    Get the name of the compiled jar file

    :return: str
    """
    path = './target'
    only_files = [f for f in listdir(path) if isfile(join(path, f))]
    jar_files = [file for file in only_files if file.endswith('.jar')]
    target_jar_file = jar_files[0]
    return target_jar_file


def upload_to_hdfs(username, password, schema_name, jar_name):
    """
    Upload the compiled jar file to HDFS

    :param username: str
    :param password: str
    :param schema_name: str
    :return: bool
    """
    params = '?op=CREATE&overwrite=true'
    hdfs_url = f'{HDFS_BASE_URL}/{schema_name}/{jar_name}{params}'
    upload_command_1 = ['curl', '-i', '-u', f'{username}:{password}', '-X', 'PUT', '-k', hdfs_url]
    bytes_output = subprocess.Popen(upload_command_1, stdout=subprocess.PIPE).communicate()
    try:
        location = bytes_output[0].decode('utf-8').split('\n')[-6]
    except Exception:
        raise Exception(colored('Please make sure to check your network settings', 'red'))
    assert('location' in location.lower())
    location_url = location.split('Location: ')[1].strip()
    upload_command_2 = ['curl', '-i', '-u', f'{username}:{password}', '-X', 'PUT', '-k', '-T',
                        f'target/{jar_name}', f'{location_url}{params}']
    bytes_output = subprocess.Popen(upload_command_2, stdout=subprocess.PIPE).communicate()
    decoded_response = bytes_output[0].decode('utf-8')
    if 'HTTP/1.1 201 Created' in decoded_response:
        return True
    return False


def create_hive_udfs(username, password):
    """
    Execute the Hive queries needed to create
    the custom Hive functions

    :param username: str
    :param password: str
    :return: bool
    """
    try:
        conn = jaydebeapi.connect('org.apache.hive.jdbc.HiveDriver',
                                JDBC_URL,
                                [username, password],
                                ABSOLUTE_JAR_FILE_PATH,
                                '')
    except Exception as e:
        print(e)
        raise Exception(colored('Something happened.\nYou fat-fingered your password\nOR\nJar file or cert not found.', 'red'))

    # Read file of commands that need to be run
    if not isfile('udf_queries.txt'):
        raise Exception(colored('Can\'t find ./udf_queries.txt', 'red'))
    text_file = open('udf_queries.txt', 'r')
    queries = text_file.readlines()
    queries = [query.strip().replace(';', '') for query in queries if query and query != '\n']

    # Execute the queries
    cur = conn.cursor()
    for query in queries:
        try:
            cur.execute(query)
        except Exception as e:
            raise Exception(e)
    cur.close()
    return True


def deploy_udf(schema_name):
    """
    Main function that deploys the whole project

    :param schema_name: str
    :return: None
    """
    username = input('Username: ')
    password = getpass.getpass()
    # Compile jar file
    if compile_jar_file():
        print(colored('SUCCESS: Jar file compiled', 'green'))
    else:
        raise Exception(colored('There was an error compiling the jar file. Use your editor of choice to debug the Maven project.', 'red'))

    # Find compiled jar file
    jar_name = get_jar_file_name()
    if jar_name:
        print(colored(f'SUCCESS: Found jar file "{jar_name}"', 'green'))
    else:
        raise Exception(colored('No jar file found', 'red'))

    # Upload jar to HDFS
    if upload_to_hdfs(username, password, schema_name, jar_name):
        print(colored('SUCCESS: Jar file uploaded', 'green'))
    else:
        raise Exception(colored('Jar file failed to upload. Check your network settings.', 'red'))

    # Execute all queries to create Hive UDFs
    if create_hive_udfs(username, password):
        print(colored('SUCCESS: All Hive UDFs created', 'green'))

    return None


HDFS_BASE_URL = 'GIVE ME A VALUE'
JDBC_URL = 'GIVE ME A VALUE'
ABSOLUTE_JAR_FILE_PATH = 'GIVE ME A VALUE'


if __name__ == '__main__':
    fire.Fire(deploy_udf)
