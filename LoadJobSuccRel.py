import argparse, csv, socket, struct, time
#from time		import time
from py2neo 	import neo4j, rel


	
DEFAULT_BATCH_SIZE = 10000

def main():
	start_time = time.time()
	parser = argparse.ArgumentParser()
	parser.add_argument('ifile', help='the csv file to load')
	parser.add_argument('-b', '--batch', type=int, default=DEFAULT_BATCH_SIZE,
        help='set batch size in terms of rows (default=%i)' % DEFAULT_BATCH_SIZE)
	args = parser.parse_args() 
	graph_db = connect()	
	
	load_file(args.ifile, args.batch, graph_db)
	elapsed_time = time.time() - start_time
	
	print 'elapsed time = %i' % elapsed_time
	
def connect():
    try:
        #graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
		neo4j.authenticate("jobscope.sb01.stations.graphenedb.com:24789",
                   "JobScope", "0W07c5PCLYr4yxPDd9ir")

		graph_db = neo4j.GraphDatabaseService("http://jobscope.sb01.stations.graphenedb.com:24789/db/data/")
    except rest.ResourceNotFound:
        print 'Database service not found'
    return graph_db
 
 
def load_file(ifile, bsize, gdb):
 
    #print 'loading batches of %i...' % bsize
 
    with open(ifile, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        #reader.next()  # skip header
        rowbuffer = []
 
        for row in reader:
            rowbuffer.append(row)
 
            if len(rowbuffer) >= bsize:
                load_batch(rowbuffer, gdb)
                rowbuffer = []
 
        if len(rowbuffer) > 0:
            load_batch(rowbuffer, gdb)
 
 
def load_batch(rows, graph_db):
 
    print "loading %i rows..." % len(rows)
    batch = neo4j.WriteBatch(graph_db)  # batch is linked to graph database
	
    for row in rows:
		pred, succ = row
		#print "%s %s" % (pred, succ)
		for pred_node in graph_db.find('Job', 'jobname', pred):
			
			print 'pred_node name is %s' % pred_node["jobname"]
		for succ_node in graph_db.find("Job", 'jobname', succ):
			print 'succ_node name is %s' % succ_node["jobname"]

		batch.create(rel(pred_node, "SUCCESSOR", succ_node))
    print 'Ok'
    batch.run()
 
    
 
 
if __name__ == '__main__':
    main()	
	
	
######################################################################################	




