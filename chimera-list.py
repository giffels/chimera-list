#!/usr/bin/env python

import os, sys, time, logging, optparse

log = logging.getLogger('chimera-dump')
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

# Parse command line options
parser = optparse.OptionParser(version='chimera-list 0.3',
	description='Tool to list all files in a dCache chimera database')
parser.add_option('-H', '--host', dest = 'host', default = 'localhost',
	help = 'Name of database host [default: localhost]')
parser.add_option('-p', '--port', dest = 'port', default = 5432,
	help = 'Port for database connection [default: 5432]')
parser.add_option('-D', '--database', dest = 'database', default = 'chimera',
	help = 'Name of database [default:chimera]')
parser.add_option('-U', '--username', dest = 'username', default = None,
	help = 'Username for database connection')
parser.add_option('-P', '--password', dest = 'password', default = None,
	help = 'Password for database connection')
outputfn_default = 'chimera_%s' % time.strftime('%Y-%m-%d_%H%M', time.localtime())
parser.add_option('-o', '--output', dest = 'output', default = outputfn_default,
	help = 'Name of outputfile [default: YYYY-mm-dd_HHMM]')
parser.add_option('-s', '--string', dest = 'pat', default = None,
	help = 'String applied on output: Either path like /store/mc or pool like f01-123-123')
parser.add_option('-r', '--root', dest = 'root', default = '/pnfs',
	help = 'Name of dCache root directory [default: /pnfs]')
parser.add_option('-R', '--raw', action = 'store_true', dest = 'raw', default = False,
	help = 'Skip postprocessing steps and output raw file list')
parser.add_option('-d', '--debug', action = 'store_true', dest = 'debug', default = False,
	help = 'Debug modus')
(opts, args) = parser.parse_args()
if opts.debug:
	log.setLevel(logging.DEBUG)

# Read username and password from file
if (opts.username == None) or (opts.password == None):
	cfgPath = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), 'chimera-list.conf'))
	try:
		import json
		try:
			config = json.load(open(cfgPath))
		except:
			sys.exit('Unable to read config file %s' % cfgPath)
	except:
		log.warning('json module not found - using fallback config reader!')
		try:
			config = eval(open(cfgPath).read())
		except:
			sys.exit('Unable to read config file %s' % cfgPath)
	if (opts.username == None) and ('username' not in config):
		sys.exit('"username" missing as argument or in config file %s!' % cfgPath)
	if (opts.password == None) and ('password' not in config):
		sys.exit('"password" missing as argument or in config file %s!' % cfgPath)

# Small tool to cleanup temp files
def unlink_safe(fn):
	try:
		if os.path.exists(fn):
			os.unlink(fn)
	except:
		pass

# Returns connection to the chimera database
def connect_db(host, port, db, user, passwd):
	try:
		log.info('Trying to connect with pgdb...')
		import pgdb
		return pgdb.connect(host='%s:%i' % (host,port), database=db, user=user, password=passwd)
	except ImportError:
		log.critical('No pgdb module found, might help to install postgresql-python')
	except:
		log.critical('pgdb: Connection to database failed')
	try:
		log.info('Trying to connect with psycopg2...')
		import psycopg2
		return psycopg2.connect('dbname=%s user=%s host=%s port=%i password=%s' % (db, user, host, port, passwd))
	except ImportError:
		log.critical('No psycopg2 module found, might help to install python-psycopg2')
	except:
		log.critical('psycopg2: Connection to database failed')
		raise

con = connect_db(opts.host, opts.port, opts.database,
	config.get('username', opts.username), config.get('password', opts.password))
if not con:
	sys.exit('Connection to database failed')

# Execute SQL and iterate over results
def get_rows(con, cmd, itersize = 5000):
	cur = con.cursor()
	cur.execute(cmd)
	while True:
		results = cur.fetchmany(itersize)
		if not results:
			break
		for result in results:
			yield result

root_magic = '000000000000000000000000000000000000'

def pfnsid2inumber(pnfsid):
	cmd = "select pnfsid2inumber('%s')" % pnfsid
	return get_rows(con, cmd).next()[0]

root_inumber_magic = pfnsid2inumber(root_magic)

# Get the inumber of the given root directory
def get_root(rootpath, parent = root_inumber_magic):
	rootpath = rootpath.strip('/').split('/', 1)
	rootdir = rootpath[0]
	cmd = "select path2inumber(%s, '%s')" % (parent, rootdir)
	try:
		result = get_rows(con, cmd).next()[0]
	except:
		sys.exit("Query for root directory '%s' failed" % rootdir)
	if len(rootpath) > 1:
		return get_root(rootpath[1], result)
	return result

root_inumber = get_root(opts.root)
log.info('Rootdir used: %s %s\n' % (opts.root, root_inumber))

# The dictionary with all dir pathes and their pnfsids as keys
dirs = {root_inumber: opts.root}

# Returns full PFN belonging to a parent inumber
def search_parent(inumber):
	if inumber not in dirs: # Use cached result if available
		cmd = "select inumber2path(%s)" % inumber
		try:
			entry = get_rows(con, cmd).next()[0]
		except:
			pass
		else:
			dirs[inumber] = entry

	return dirs.setdefault(inumber, None)

# Create raw chimera dump
def write_dump_raw(fn):
	log.info('dCache dump  started at: %s' % repr(time.localtime()[0:6]))
	cmd = "select t_inodes.ipnfsid,iname,iparent,isize,ilocation,date_part('epoch', t_inodes.iatime),isum"
	cmd += ' from t_inodes,t_locationinfo,t_dirs,t_inodes_checksum'
	cmd += ' where t_dirs.ichild = t_locationinfo.inumber and t_dirs.ichild = t_inodes.inumber and t_dirs.ichild = t_inodes_checksum.inumber'

	fp = open(fn, 'w')
	for (pnfsid, name, parent, size, location, atime, cksum) in get_rows(con, cmd):
		parent_path = search_parent(parent)
		if not parent_path:
			continue
		entry = '%s/%s\t%s\t%s\t%d\t%d\t%s\n' % (parent_path, name, pnfsid, cksum, size, atime, location)
		if opts.pat and opts.pat not in entry:
			continue
		fp.write(entry)
	log.info('dCache dump finished at: %s' % repr(time.localtime()[0:6]))
	con.close()
	fp.close()
try:
	write_dump_raw(opts.output + '.raw')
except:
	unlink_safe(opts.output + '.raw')
	sys.exit('Unable to write raw database dump!')

# The following are -> optional <- postprocessing steps
# to shrink the size of the chimera dump by a large factor
if opts.raw:
	os.rename(opts.output + '.raw', opts.output)
	sys.exit(0)

log.info('Processing   started at: %s' % repr(time.localtime()[0:6]))
# Perform external sort by PFN
try:
	os.system('export LC_ALL=C; sort %s.raw -o %s.sorted' % (opts.output, opts.output))
	unlink_safe(opts.output + '.raw')
	if not os.path.exists(opts.output + '.sorted'):
		raise Exception
except:
	unlink_safe(opts.output + '.raw')
	unlink_safe(opts.output + '.sorted')
	sys.exit('Unable to sort raw database dump!')

# Condense chimera dump - merge locations into single lines and group into directories
def write_dump_condensed(inputfn, outputfn):
	fp_out = open(outputfn, 'w')

	# Write a single entry together with a location list
	# Group entries into directories by only writing directory name if it changes
	def write_entry(entry, location_list):
		dn, other = entry.rsplit('/', 1)
		if dn != write_entry.last_dn:
			fp_out.write(dn + '\n')
			write_entry.last_dn = dn
		# simplify location list
		def short_loc(loc):
			try:
				return loc.split('group=')[1].split('&')[0]
			except:
				return loc
		fp_out.write(other + '\t' + str.join(',', map(short_loc, location_list)) + '\n')
	write_entry.last_dn = None

	# Iterate over raw entries and merge entries if the non-locality part is the same
	last_entry = None
	location_list = []
	fp_in = open(inputfn)
	for line in fp_in:
		(entry, location) = line.strip().rsplit('\t', 1)
		if last_entry and (entry != last_entry):
			write_entry(last_entry, location_list)
			location_list = []
		location_list.append(location)
		last_entry = entry
	if last_entry:
		write_entry(last_entry, location_list)
	fp_in.close()
	fp_out.close()

	unlink_safe(inputfn)
try:
	write_dump_condensed(opts.output + '.sorted', opts.output)
except:
	unlink_safe(opts.output + '.sorted')
	sys.exit('Unable to write condensed database dump!')

log.info('Processing  finished at: %s' % repr(time.localtime()[0:6]))
