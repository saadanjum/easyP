
import psycopg2
import psycopg2.extras


class easyP:
    def __init__(self, HOST, DB, USERNAME, PASSWORD, PORT=5432):
        self.__host = HOST
        self.__db = DB
        self.__username = USERNAME
        self.__password = PASSWORD
        self.__port = PORT
        self.__sqlLogging = False
        self.__connection = None
        self.__cursor = None

    
    def __create_response(self, rowcount = None, results = [], status = 'ERROR', error = 'Returned without executing'):
        return {
            'rowcount': rowcount,
            'results': results,
            'status': status,
            'error': error
        }


    def checkconnection(self):
        if self.__connection.close != 0:
            self.connect()


    def reconnect(self):
        self.disconnect()
        self.connect()


    def enableLogging(self):
        self.__sqlLogging = True


    def connect(self):
        self.__connection = psycopg2.connect(host=self.__host, database=self.__db, user=self.__username, password=self.__password, port=self.__port)
        self.__cursor = self.__connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


    def disconnect(self):
        if self.__connection:
            self.__connection.close()


    def select(self, table, select = [], where = None, orderBy = None, limit = None, offset = None, distinctOn = None):
        response = self.__create_response()
        try:
            selectSQL = """SELECT """

            if len(select) > 0:
                if distinctOn and type(distinctOn) == type("a"):
                    selectSQL += "DISTINCT ON (%s) "%distinctOn

                for select in select:
                    selectSQL += "%s, "%select

                selectSQL = selectSQL[:-2]
            else:
                selectSQL += "count(*) "

            selectSQL += " FROM %s "%table

            if where and type(where) == type({}):
                firstField = True
                for field in where:
                    selectSQL += "WHERE " if firstField else ""


                    if where[field] is None:
                        selectSQL += "%s IS NULL AND "%(field)
                    elif where[field].lower() == 'null':
                        selectSQL += "%s IS NULL AND "%(field)
                    elif where[field].lower() == 'not null':
                        selectSQL += "%s IS NOT NULL AND "%(field)
                    elif type(where[field]) == type("") and (where[field].startswith("< ") or where[field].startswith("> ") or where[field].startswith("<= ") or where[field].startswith(">= ")):
                        whereCompare = where[field].split(" ")
                        selectSQL += "%s %s %s AND "%(field, whereCompare[0], whereCompare[1])
                    else:
                        selectSQL += "%s = "%(field) + self.__cursor.mogrify("%s AND ",[where[field]])
                    firstField = False

                selectSQL = selectSQL[:-4]

            if orderBy and type(orderBy) == type([]):
                selectSQL += " ORDER BY "
                if distinctOn and type(distinctOn) == type(""):
                    selectSQL += " %s,"%distinctOn
                for field in orderBy:
                    selectSQL += " %s,"%field

                selectSQL = selectSQL[:-1]

            if limit and type(limit) == type(1):
                selectSQL += " LIMIT %s "%limit

            if offset and type(offset) == type(1):
                selectSQL += " OFFSET %s "%offset


            if self.__sqlLogging:
                print "Executing Query: %s"%selectSQL
            self.checkconnection()
            self.__cursor.execute(selectSQL)

            response = self.__create_response(
                rowcount=self.__cursor.rowcount,
                results=self.__cursor.fetchall(),
                status="OK",
                error=None
            )

        except Exception, e:
            response = self.__create_response(
                rowcount=None,
                results=None,
                status"ERROR",
                error="ERROR: %s"%e
            )

        return response


    def update(self, table, setCols = {}, where = None):
        response = self.__create_response()
        try:

            updateSQL = "UPDATE %s "%table

            if setCols and type(setCols) == type({}) and len(setCols) > 0:
                updateSQL += "SET "
                for field in setCols:
                    if field == "":
                        setCols[field] = None
                    updateSQL += "%s = "%(field) + self.__cursor.mogrify("%s, ",[setCols[field]])
                updateSQL = updateSQL[:-2]
                updateSQL += " "
            else:
                e = "set clause cannot be empty or null"
                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )

                return response

            if where and type(where) == type({}):
                firstField = True
                for field in where:
                    updateSQL += "WHERE " if firstField else ""
                    if where[field] is None:
                        updateSQL += "%s IS NULL AND "%(field)
                    elif type(where[field]) == type("") and where[field].lower() == 'null':
                        updateSQL += "%s IS NULL AND "%(field)
                    elif type(where[field]) == type("") and where[field].lower() == 'not null':
                        updateSQL += "%s IS NOT NULL AND "%(field)
                    else:
                        updateSQL += "%s = "%(field) + self.__cursor.mogrify("%s AND ",[where[field]])
                    firstField = False

                updateSQL = updateSQL[:-4]

            updateSQL += " RETURNING *"

            if self.__sqlLogging:
                print "Executing Query: %s"%updateSQL

            try:
                self.checkconnection()
                self.__cursor.execute(updateSQL)
                response = self.__create_response(
                    rowcount=self.__cursor.rowcount,
                    results=self.__cursor.fetchall(),
                    status="OK",
                    error=None
                )
                self.__connection.commit()
            except Exception, e:

                if self.__connection:
                    self.__connection.rollback()

                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )

        except Exception, e:
            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def batchInsert(self, table, insertObjects = None):
        response = self.__create_response()
        try:
            insertSQL = ""
            pairCols = ""
            pairVals = ""

            insertSQL += "Insert INTO %s "%(table)

            if insertObjects and type(insertObjects) == type([]) and len(insertObjects) > 0:
                first = True
                for valuePairs in insertObjects:
                    pairCols = ""
                    pairVals = ""
                    for field in valuePairs:
                        pairCols += "%s, "%field
                        pairVals += self.__cursor.mogrify("%s, ",[valuePairs[field]])
                    pairCols = pairCols[:-2]
                    pairVals = pairVals[:-2]

                    if first:
                        first = False
                        insertSQL += "(%s) VALUES (%s), "%(pairCols, pairVals)
                    else:
                        insertSQL += "(%s), "%(pairVals)

                insertSQL = insertSQL[:-2]
                insertSQL += " returning *"

            else:
                e = "insert needs atleast 1 pair value and cannot be None"
                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )
                return response

            if self.__sqlLogging:
                print "Executing Query: %s"%insertSQL

            try:
                self.checkconnection()
                self.__cursor.execute(insertSQL)
                response = self.__create_response(
                    rowcount = self.__cursor.rowcount,
                    results = self.__cursor.fetchall(),
                    status = "OK",
                    error = None
                )
                self.__connection.commit()
            except Exception, e:

                if self.__connection:
                    self.__connection.rollback()

                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )

        except Exception, e:
            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def insert(self, table, valuePairs = None):
        response = self.__create_response()
        try:
            insertSQL = ""
            pairCols = ""
            pairVals = ""

            if valuePairs and type(valuePairs) == type({}) and len(valuePairs) > 0:
                for field in valuePairs:
                    pairCols += "%s, "%field
                    pairVals += self.__cursor.mogrify("%s, ",[valuePairs[field]])
                pairCols = pairCols[:-2]
                pairVals = pairVals[:-2]
            else:
                e = "insert needs atleast 1 pair value and cannot be None"
                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )
                return response

            insertSQL += "Insert INTO %s (%s) VALUES (%s) RETURNING * "%(table, pairCols, pairVals)

            if self.__sqlLogging:
                print "Executing Query: %s"%insertSQL

            try:
                self.checkconnection()
                self.__cursor.execute(insertSQL)
                response = self.__create_response(
                    rowcount = self.__cursor.rowcount,
                    results = self.__cursor.fetchall(),
                    status = "OK",
                    error = None
                )
                self.__connection.commit()
            except Exception, e:

                if self.__connection:
                    self.__connection.rollback()

                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )

        except Exception, e:
            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def delete(self, table = None, where = None):
        response = self.__create_response()
        try:
            if table is None:
                response = self.__create_response(
                    rowcount = None,
                    results = None,
                    status = "ERROR",
                    error = "Table name not provided"
                )
                return response

            deleteSQL = "DELETE FROM %s "%table

            if where and type(where) == type({}):
                firstField = True
                for field in where:
                    deleteSQL += "WHERE " if firstField else ""
                    if where[field] is None:
                        deleteSQL += "%s IS NULL AND "%(field)
                    elif where[field].lower() == 'null':
                        deleteSQL += "%s IS NULL AND "%(field)
                    elif where[field].lower() == 'not null':
                        deleteSQL += "%s IS NOT NULL AND "%(field)
                    else:
                        deleteSQL += "%s = "%(field) + self.__cursor.mogrify("%s AND ",[where[field]])
                    firstField = False

                deleteSQL = deleteSQL[:-4]

            if self.__sqlLogging:
                print "Executing Query: %s"%deleteSQL

            try:
                self.checkconnection()
                self.__cursor.execute(deleteSQL)
                response = self.__create_response(
                    rowcount = self.__cursor.rowcount,
                    results = None,
                    status = "OK",
                    error = None
                )
                self.__connection.commit()
            except Exception, e:

                if self.__connection:
                    self.__connection.rollback()

                response = self.__create_response(
                    rowcount= None,
                    results= None,
                    status= "ERROR",
                    error= "ERROR: %s"%e
                )

        except Exception, e:
            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def executeRawQuery(self, queryString, arguments=[]):
        response = self.__create_response()
        try:
            self.checkconnection()
            self.__cursor.execute(self.__cursor.mogrify(queryString, arguments))
            
            response = self.__create_response(
                rowcount = self.__cursor.rowcount,
                results = None,
                status = "OK",
                error = None
            )

        except Exception, e:
            if self.__connection:
                self.__connection.rollback()

            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def queryFetchall(self):
        response = self.__create_response()
        try:
            response = self.__create_response(
                rowcount = self.__cursor.rowcount,
                results = self.__cursor.fetchall(),
                status = "OK",
                error = None,
            )

        except Exception, e:
            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response


    def queryCommit(self):
        response = self.__create_response()
        try:
            self.__connection.commit()
            response = self.__create_response(
                rowcount = None,
                results = None,
                status = "OK",
                error = None
            )

        except Exception, e:

            if self.__connection:
                self.__connection.rollback()

            response = self.__create_response(
                rowcount= None,
                results= None,
                status= "ERROR",
                error= "ERROR: %s"%e
            )

        return response
