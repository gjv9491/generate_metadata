from sqlalchemy import inspect, create_engine, dialects
from psycopg2 import sql, extras
import json
import logging
from os import sys, path, makedirs
import argparse



class get_metada(object):
    def __init__(self, ENV):
        self.config = json.load(open('creds.json'))
        self.user = self.config[ENV]['user']
        self.password = self.config[ENV]['password']
        self.host = self.config[ENV]['host']
        self.port = self.config[ENV]['port']
        self. dbname = self.config[ENV]['dbname']
        self.db_uri = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}" \
                      f"?keepalives=1&keepalives_idle=200&keepalives_interval=200&keepalives_count=5"
        logging.debug(self.db_uri)
        self.engine = create_engine(self.db_uri)
        self.inspector = inspect(self.engine)
        logging.debug(self.inspector)
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor(cursor_factory=extras.RealDictCursor)

    def generate_create_table(self, table_script, **kw):
        _table_name=kw.get("table_name", None)
        _schema_name=kw.get("schema_name", None)
        if _table_name is None or _schema_name is None:
            logging.info(f"{_table_name} and {_schema_name} cannot be None")

        __create_table_script = f"CREATE TABLE {_schema_name}.{_table_name} ("

        if table_script:
            logging.info('columns not in table script')

        for col in table_script:
            not_null, default=('',)*2
            #if col['autoincrement']:
            #    autoincrement='serial'
            if col['nullable'] is False:
                not_null = ' NOT NULL'
            elif col['default'] is not None:
                default = f" DEFAULT {col['default']}"

            __create_table_script+=f"{str(col['name'])} {str(col['type'])} {not_null} {default}, "
        __create_table_script=__create_table_script.rstrip()[:-1]+");"
        logging.debug(__create_table_script)
        return __create_table_script

    def generate_add_constraints(self, constraint_script,**kw):
        _table_name=kw.get("table_name", None)
        _schema_name=kw.get("schema_name", None)
        __alter_script=''
        if _table_name is None or _schema_name is None:
            logging.info(f"{_table_name} and {_schema_name} cannot be None")

        if isinstance(constraint_script, list):
            for foreign_keys in constraint_script:
                foreign_key_identifiers=['referred_schema', 'referred_table', 'referred_columns']
                if all(key in foreign_keys for key in foreign_key_identifiers):
                    __alter_script += f"ALTER TABLE {_schema_name}.{_table_name} ADD CONSTRAINT {foreign_keys['name']} " \
                                     f"FOREIGN KEY ({','.join(foreign_keys['constrained_columns'])}) " \
                                     f"REFERENCES {foreign_keys['referred_schema']}.{foreign_keys['referred_table']}({','.join(foreign_keys['referred_columns'])});"
                    logging.info('generating alter statement for foreign key constraints')
                    logging.debug(__alter_script)
                else:
                    __alter_script += f"ALTER TABLE {_schema_name}.{_table_name} ADD CONSTRAINT {foreign_keys['name']} " \
                                     f"UNIQUE ({','.join(foreign_keys['column_names'])});"
                    logging.debug(__alter_script)
                    logging.info('generating alter statement for unique key constraints')
        elif isinstance(constraint_script, dict):
            if constraint_script['constrained_columns']:
                __alter_script += f"ALTER TABLE {_schema_name}.{_table_name} ADD CONSTRAINT {constraint_script['name']} " \
                                  f"PRIMARY KEY ({','.join(constraint_script['constrained_columns'])});"
                logging.debug(__alter_script)
        else:
            logging.info('constraint script cannot be empty')

        return __alter_script

    def generate_table_json(self, table_script, **kw):
        __json_table={}
        __table_column={}
        _table_name = kw.get("table_name", None)
        _schema_name = kw.get("schema_name", None)
        if _table_name is None or _schema_name is None:
            logging.info(f"{_table_name} and {_schema_name} cannot be None")
            raise

        for column in table_script:
            column['default'] = None if column['default'] is not None and 'nextval' in column['default'] else column['default']
            __table_column[column['name']] = str(column)

        __json_table[f"{_table_name}"]=__table_column
        logging.info(f"processing {_schema_name}.{_table_name}")
        return __json_table

    def get_list_of_tables_in_schema(self, schema_name):
        return [table for table, foreign_keys in self.inspector.get_sorted_table_and_fkc_names(schema=schema_name) if table]

    def get_schema_and_table_in_json(self, schema_name, connection_object):
        _json_per_schema={}
        content={}
        list_of_tables= connection_object.get_list_of_tables_in_schema(schema_name=schema_name)
        for table in list_of_tables:
            record = connection_object.generate_table_json(table_script=self.inspector.get_columns(table_name=table, schema=schema_name),
                                       table_name=table,
                                       schema_name=schema_name)
            content[list(record.keys())[0]] = list(record.values())[0]
        _json_per_schema[schema_name] = content
        return _json_per_schema

    def get_constraints(self, schema_name, method_name, connection_object):
        _json_per_schema = {}
        _table_unique = {}
        list_of_tables = connection_object.get_list_of_tables_in_schema(schema_name=schema_name)
        for table in list_of_tables:
            _table_unique[f"{table}"] = method_name(table_name=table, schema=schema_name)
            logging.info(f"Constraints are being written out for {schema_name}.{table}")
        _json_per_schema[schema_name]=_table_unique
        return _json_per_schema


    def write_file_out(self, file_path, file_name, content):
        if not any([file_path,file_name]):
            logging.info(f"file path: {file_path} and file name {file_name} cannot be empty")
        if not path.exists(file_path):
            makedirs(file_path)
            logging.info(f"creating path {file_path}")
        logging.info(path.join(file_path, file_name))
        with open(path.join(file_path,file_name),'w+') as myfile:
            myfile.write(content)
        logging.info(f"Content written to {file_name} in {file_path} complete ")




def main():
    parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--loglevel", type=str, default="INFO")
    parser.add_argument("--env", choices=['dev', 'stg', 'prod'], required=True)
    parser.add_argument("--schema", nargs="*", type=str, default=[])

    # if len(sys.argv) == 1:
    #     parser.print_help()
    # sys.exit(1)

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel, format='%(asctime)s %(levelname)s %(funcName)s:%(lineno)d %(message)s',
                        stream=sys.stdout)
    metadata = get_metada(ENV=args.env)

    #Exclude a list of schemas
    exclude_schema_list=['public']
    schemas_all = [sch for sch in metadata.inspector.get_schema_names() if sch not in exclude_schema_list]

    #Verify all the schema names passed in, is present in the db
    schema_in_args = [items for items in args.schema if items in schemas_all]
    schema_not_in_args = [items for items in args.schema if items not in schemas_all]


    final_list_of_schemas=schemas_all
    if schema_in_args:
        final_list_of_schemas=schema_in_args

    logging.info(f"List of schemas present in db {final_list_of_schemas}")
    logging.info(f"list of schemas not present in db {schema_not_in_args}")


    for schema in final_list_of_schemas:
        #TODO write create table json
        json_data = json.dumps(metadata.get_schema_and_table_in_json(schema_name=schema, connection_object=metadata), indent=4)
        metadata.write_file_out(file_path='ddl', file_name=f"{schema}.json", content=json_data)

        #TODO write create primary key constraints
        json_pk_data = json.dumps(metadata.get_constraints(schema_name=schema, method_name=metadata.inspector.get_pk_constraint, connection_object=metadata), indent=4)
        metadata.write_file_out(file_path='constraint/primary_key', file_name=f"{schema}.json", content=json_pk_data)

        #TODO write create foreign key contraints
        json_fk_data = json.dumps(metadata.get_constraints(schema_name=schema, method_name=metadata.inspector.get_foreign_keys, connection_object=metadata), indent=4)
        metadata.write_file_out(file_path='constraint/foreign_key', file_name=f"{schema}.json", content=json_fk_data)

        #TODO write create unique key contraints
        json_unique_data = json.dumps(metadata.get_constraints(schema_name=schema, method_name=metadata.inspector.get_unique_constraints, connection_object=metadata), indent=4)
        metadata.write_file_out(file_path='constraint/unique_key', file_name=f"{schema}.json", content=json_unique_data)

    if metadata.connection:
        logging.info(metadata.connection)
        metadata.connection.close()


if __name__ == '__main__':
    exit(main())