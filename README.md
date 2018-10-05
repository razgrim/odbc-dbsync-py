# odbc-dbsync-py
Sync tables across databases via ODBC connections. 
This project is meant to be a simple, unintrusive (however less efficient) alternative to trigger based sync solutions
like SymmetricDS. 
Meant for infrequent synchronizations on proprietary databases where trigger/table changes/writing could be problematic.
Not designed for "real time" sync, it can compare several thousand rows/sec on basic hardware.  
