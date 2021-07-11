import uuid
import random
from datetime import datetime, timedelta
import sys

cql_header = """
DROP KEYSPACE IF EXISTS test;
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;
CREATE TABLE IF NOT EXISTS test.data_by_date (
    tenant_uuid uuid,
    metric_id bigint,
    org_id bigint,
    emp_id bigint,
    date timestamp,
    metric_val double,
    ts timeuuid,
    PRIMARY KEY ((tenant_uuid, metric_id, org_id, emp_id), date, ts)
);
"""

insert_template = ["INSERT INTO test.data_by_date (tenant_uuid, metric_id, org_id, emp_id, date, metric_val, ts) VALUES (",");\n"]

def gen_datetime(min_year=1900, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()

def build_cql(tenants, rows, filename, header):
    with open(filename, "w") as f:
        rows_per_tenant = int(rows/tenants)
        #print(cql_header)
        #print(header)
        if header.lower() in ['header','true','1']:
            f.write(cql_header)
        for i in range(tenants):
            tenant_uuid = str(uuid.uuid4())
            for j in range(rows_per_tenant):
                metric_id = str(random.randint(0,100))
                org_id = str(random.randint(0,20))
                emp_id = str(random.randint(0,50000))
                metric_val = str(random.randint(-100,100))
                timestamp = str(gen_datetime(2019).replace(hour=0,minute=0,second=0,microsecond=0))
                ts = str(uuid.uuid1())
                insert_string = tenant_uuid+", "+metric_id+", "+org_id+", "+emp_id+", '"+timestamp+"', "+metric_val+", "+ts
                insert_line = insert_template[0]+insert_string+insert_template[1]
                #print(insert_line)
                f.write(insert_line)

if __name__ == "__main__":
    num_tenants = int(sys.argv[1])  #number of tenant ids
    total_rows = int(sys.argv[2])   #total number of rows to create
    out_file = sys.argv[3]          #name of file to output
    header = sys.argv[4]            #header/true/1 if desired to add the table creation header to the cql file
    build_cql(num_tenants,total_rows,out_file, header)
