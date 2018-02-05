from configparser import ConfigParser


def get_postgres_config(filename='database.ini', section='postgresql'):
    """
        Parse postgres config from formatted ini file
        Args:
            filename (str): Name of ini file to parse, defaults to
                'database.ini'
            section (str): Section name of file to parse as postgres config,
                defaults to 'postgresql'
        Returns:
            postgres_config (dict): param key to param value
    """

    parser = ConfigParser()
    parser.read(filename)

    postgres_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            postgres_config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
            section, filename))

    return postgres_config
