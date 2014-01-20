import argparse, csv, socket, struct
from time		import time
from py2neo 	import neo4j, rel

JOB_INDEX         = 'job_index'
SECTION_INDEX     = 'schedule_index'
OWNS_INDEX 	      = 'owns_index'
SUCCESSOR_INDEX   = 'successor_index'	
	
DEFAULT_BATCH_SIZE = 10000

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('ifile', help='the csv file to load')
	parser.add_argument('-b', '--batch', type=int, default=DEFAULT_BATCH_SIZE,
        help='set batch size in terms of rows (default=%i)' % DEFAULT_BATCH_SIZE)
	args = parser.parse_args() 
	graph_db = connect()	
	

	graph_db.get_or_create_index(neo4j.Relationship, OWNS_INDEX)

	
	load_file(args.ifile, args.batch, graph_db)
	
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
 
    print 'loading batches of %i...' % bsize
 
    with open(ifile, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        reader.next()  # skip header
        rowbuffer = []
 
        for row in reader:
            rowbuffer.append(row)
 
            if len(rowbuffer) >= bsize:
                load_batch(rowbuffer, gdb)
                rowbuffer = []
 
        if len(rowbuffer) > 0:
            load_batch(rowbuffer, gdb)
 
 
def load_batch(rows, graph_db):
 
    print "%10d  loading %i rows..." % (time(), len(rows))
    batch = neo4j.WriteBatch(graph_db)  # batch is linked to graph database
 
    for row in rows:
		schedule, job = row
		#print 'section = %s  job = %s' % (section, job)
		schedule_node = graph_db.get_indexed_node("Schedule", 'name', schedule)
		#schedule_node = graph_db.find("Schedule", 'name', schedule)
		#print 'schedule_node = %i' % schedule_node._id
		job_node = graph_db.get_indexed_node("Job", 'jobname', job)
		#job_node = graph_db.find("Job", 'jobname', job)
		#print ' %i OWNS %i' % (schedule_node._id, job_node._id)
		batch.create(rel(schedule_node, "OWNS", job_node))
		#batch.get_or_create_indexed_relationship(SUCCESSOR_INDEX, 'type', "SUCCESSOR", pred_job, "SUCCESSOR", succ_job, {})
		
    batch.run()
 
    
 
 
if __name__ == '__main__':
    main()	
	
	
######################################################################################	




