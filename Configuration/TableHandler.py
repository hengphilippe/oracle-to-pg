from configparser import ConfigParser
from distutils.command.config import config
from datetime import datetime
import shutil


class TableHandler:
    def __init__(self, source_name) -> None:
        self.source_name = source_name
        self.config_file_name = "Configuration/" + self.source_name + ".ini"
        self.config_object = ConfigParser()
        self.config_object.read(self.config_file_name)
        if not self.config_object.has_section(self.source_name):
            print("⚠️  Not configuration exist")

    def getZeroSCN(self):
        tables = []
        for table, scn in self.config_object.items(self.source_name):
            if(int(scn) == 0):
                tables.append(table)
            # print(' {} = {}'.format(table, scn))
        return tables

    def getSCN(self, tablename):
        tableSCN = self.config_object.get(
            self.source_name, tablename, fallback=False)
        if(tableSCN):
            return tableSCN

    def getMinSCN(self):
        data = []
        for key, value in self.config_object.items(self.source_name):
            data.append(value)
        return min(data)

    def setSCN(self, tablename, scn):
        # self.config_object.read(self.config_file_name)
        tableSCN = self.config_object.get(
            self.source_name, tablename, fallback=False)
        if(tableSCN):
            self.config_object.set(self.source_name, tablename, scn)
            # Write changes back to file
            with open(self.config_file_name, 'w') as conf:
                self.config_object.write(conf)
                print("-->  scn for : " + tablename + " has been set")
        else:
            print("⚠️  config not found for : " + tablename)

    def setSCNAll(self, newSCN):
        for seg, scn in self.config_object.items(self.source_name):
            self.config_object.set(self.source_name, seg, newSCN)
        with open(self.config_file_name, 'w') as conf:
            self.config_object.write(conf)

    def getTables(self):
        tables = []
        for table, scn in self.config_object.items(self.source_name):
            tables.append(table)
        return tables

    def getAllSchemas_Tables(self):
        schemas = []
        tables = []
        for seg, scn in self.config_object.items(self.source_name):
            schema = seg.split(".")[0]
            if schema not in schemas:
                schemas.append(schema.upper())
            table = seg.split(".")[1]
            if table not in tables:
                tables.append(table.upper())

        print(tables)
        return schemas, tables

    def archive(self):
        now = datetime.now()
        filename = "Configuration/archive/" + self.source_name + \
            "_" + now.strftime("%m%d%Y-%H%M%S") + ".arc"

        shutil.copyfile(self.config_file_name, filename)

if __name__ == "__main__":
    x = TableHandler("ASYWDB")
    x.getAllSchema()
