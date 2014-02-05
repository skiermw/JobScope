# all the imports
from py2neo import neo4j, rel
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash



# configuration
PRED_QUERY = "MATCH sched-[:OWNS]->p-[:SUCCESSOR]->(n:Job {jobname: %s}) RETURN sched.name as sched, p.jobname as pred"
SUCC_QUERY = "MATCH (n:Job {jobname:%s})-[:SUCCESSOR]->(s)<-[:OWNS]-sched RETURN s.jobname as succ, sched.name as sched"
#DEBUG = True
SECRET_KEY = 'development key'

start_job =""

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)
#app.config.from_envvar('FLASKR_SETTINGS', silent=True)


def connect_db():
    try:
        #graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
		neo4j.authenticate("jobscope.sb01.stations.graphenedb.com:24789",
                   "JobScope", "0W07c5PCLYr4yxPDd9ir")

		graph_db = neo4j.GraphDatabaseService("http://jobscope.sb01.stations.graphenedb.com:24789/db/data/")
    except rest.ResourceNotFound:
        print 'Database service not found'
    return graph_db
		
def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()	
		
@app.before_request
def before_request():
    graph_db = connect_db()

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

@app.route('/')
def show_jobs():
	#start_job = "'PAP080'"
	print 'start_job %s' % start_job
	if start_job <> '':
		pred_query = PRED_QUERY % start_job.upper()
		succ_query = SUCC_QUERY % start_job.upper()
		
		graph_db = connect_db()
		#predessors = []
		#successors = []
		p_query = neo4j.CypherQuery(graph_db, pred_query)
		s_query = neo4j.CypherQuery(graph_db, succ_query)
		#predessors = [dict(jobname=result.p_jobname, owner=result.o_name) for result in p_query.stream()]
		predessors = [dict(jobname=result.pred, owner=result.sched) for result in p_query.stream()]
		successors = [dict(jobname=result.succ, owner=result.sched) for result in s_query.stream()]
		
	else:
		predessors = ""
		successors = ""
	#print 'predessors = %s' % predessors
	#print 'successors = %s' % successors
	
	print 'Num predessors %i' % len(predessors)
	print 'Num successors %i' % len(successors)
	
	return render_template('show_jobs.html',successors=successors, predessors=predessors)

@app.route('/submit', methods=['POST'])
def submit_job():
	global start_job
	
	start_job = "'%s'" % request.form['jobname']
	flash('%s' % request.form['jobname'].upper())
	print 'submit job %s' % start_job
	
	return redirect(url_for('show_jobs'))
	
@app.route('/submit_link/<jobname>')
def submit_job_link(jobname):
	global start_job
	
	start_job = "'%s'" % jobname
	flash('%s' % jobname)
	print 'submit job %s' % start_job
	
	return redirect(url_for('show_jobs'))
if __name__ == '__main__':
    app.run(host='0.0.0.0')
	#app.run()
