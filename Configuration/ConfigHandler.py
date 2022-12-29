# Handle Operation for Configuration
import json
from configparser import ConfigParser


class Connector:
    def __init__(self, filename='Configuration/connector.json') -> None:
        self.fileName = filename
        with open(self.fileName, "r") as data:
            self.config_data = json.load(data)
        data.close()

    def loadNewTables(self, connectorName):
        # Get the configparser object
        tables_configuation = ConfigParser()
        tables = self.config_data[connectorName]["tables"]
        if(len(tables) > 0):
            data = {}
            for table in tables:
                data[table["name"]] = table["SCN"]

            tables_configuation[connectorName] = data
            fileTableName = connectorName + '.ini'
            with open(fileTableName, 'w') as conf:
                tables_configuation.write(conf)

            # clear tables
            tables.clear()
            print(self.config_data)
            with open(self.fileName, "w") as jsonfile:
                new_config_data = json.dump(self.config_data, jsonfile)
                jsonfile.close()
        else:
            print(" - No new tables add for connector {}".format(connectorName))

    def getTableZeroSCN(self, connectorName):
        tables = self.getNewTables(connectorName)
        tblFresh = []
        for table in tables:
            if(int(table['SCN']) == 0):
                tblFresh.append(table['name'])
        return tblFresh

    def getInitConnector(self):
        for connector in self.config_data:
            freshTable = self.getTableZeroSCN
            if(len(freshTable) > 0):
                return
            print(connector)
    
    # def setTableSCN(self, connectorName, tablename, scn) -> None:
    #     for table  in self.config_data[connectorName]['tables']:
    #         # print(table['name'], "->" , tablename)
    #         if(str(table['name']) == tablename):
    #             table['SCN'] = scn
    #         else:
    #             print("something wrong with tablename")
    #     with open(self.fileName, "w") as jsonfile:
    #         new_config_data = json.dump(self.config_data, jsonfile)
    #         print("scn updated.")
    #         jsonfile.close()


if __name__ == "__main__":
    connect = Connector('connector.json')
    connect.loadNewTables("ASYWDB")
    connect.loadNewTables("ECUSDB")
