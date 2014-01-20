import argparse, csv, socket, struct, json
from time		import time
from py2neo 	import neo4j, rel




def main():
	start_job = "'PAP080'"
	depth = "5"
	pred_query = "start n=node:job_index(jobname =  %s ) match o-[:OWNS]->p<-[:SUCCESSOR]-(n) return p.jobname, o.name" % start_job
	SUCC_QUERY = "start n=node:job_index(jobname = %s) match n-[:SUCCESSOR*..%s]->(s)"
	succ_query = SUCC_QUERY % (start_job, depth)
	graph_db = connect()	
	#pred_query = pred_query % start_job
	query = neo4j.CypherQuery(graph_db, succ_query) 
	for result in query.stream():
		#print result.p_jobname, result.o_name
		entries = dict(jobname=result.p_jobname, owner=result.o_name)
	
	print entries
	
	
	
	
	

	
def connect():
    try:
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    except rest.ResourceNotFound:
        print 'Database service not found'
    return graph_db
 
 

 
if __name__ == '__main__':
    main()	
	
	
######################################################################################	




