chimera-list
============

Tool to dump the content of the chimera namespace service to a text file for further processing.
It is *HIGHLY* recommended to supply the username and password using a config file
(located in the same directory as the executable). The config file is named
`chimera-list.conf` and uses the JSON format:
```JSON
{
    "username": "postgres",
    "password": ""
}
```

Usage
-----

```
Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -H HOST, --host=HOST  Name of database host [default: localhost]
  -p PORT, --port=PORT  Port for database connection [default: 5432]
  -D DATABASE, --database=DATABASE
                        Name of database [default:chimera]
  -U USERNAME, --username=USERNAME
                        Username for database connection
  -P PASSWORD, --password=PASSWORD
                        Password for database connection
  -o OUTPUT, --output=OUTPUT
                        Name of outputfile [default: YYYY-mm-dd_HHMM]
  -s PAT, --string=PAT  String applied on output: Either path like /store/mc
                        or pool like f01-123-123
  -r ROOT, --root=ROOT  Name of dCache root directory [default: /pnfs]
  -R, --raw             Skip postprocessing steps and output raw file list
  -d, --debug           Debug modus
```

The repository also includes the bash script `chimera-cron.sh` to transfer the
dump file to another host.

Dependencies: pgdb, psycopg2, python >= 2.4
