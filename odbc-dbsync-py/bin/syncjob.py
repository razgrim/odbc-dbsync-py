class syncjob(object):
    """Carries variables read in from sync files, as well as sync methods."""
    connectionString1
    connectionString2
    tableMaps

    def __init__(self):
        None

class tablemap(object):
    #1 for 1->2, 2 for 2->1, 3 for 1<->2
    direction
    table1
    table2

class table(object):
    tableName
    pkCol
    modTimeCol #will be used to determine tiebreaker in direction #3
