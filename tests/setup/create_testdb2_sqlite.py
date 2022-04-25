from sqlite3 import connect, Row

from test_utils import get_testdb_url
from tlbx import st, rmfile, shell

CREATE_AGGREGATED_STATS = """CREATE TABLE aggregated_stats (
partner_id int,
campaign_id int,
leads int default 0,
sales int default 0,
revenue decimal(10,2) default 0.0,
primary key (partner_id, campaign_id))
"""

LOAD_AGGREGATED_STATS = """REPLACE INTO aggregated_stats
SELECT
c.partner_id,
c.id as campaign_id,
count(distinct l.id) as leads,
count(distinct s.id) as sales,
sum(s.revenue) as revenue
from testdb1.sales s
join testdb1.leads l on s.lead_id=l.id
join testdb1.campaigns c on l.campaign_id=c.id
group by 1,2
"""

if __name__ == "__main__":
    rmfile("testdb2", ignore_missing=True)
    shell("touch testdb2")

    conn = connect("testdb2")
    cursor = conn.cursor()

    with open("common.sqlite.sql", "r") as sql_file:
        sql_script = sql_file.read()
    cursor.executescript(sql_script)
    conn.commit()

    cursor.execute('ATTACH DATABASE "testdb1" AS testdb1')
    cursor.execute("REPLACE into partners SELECT * FROM testdb1.partners")
    cursor.execute("REPLACE into campaigns SELECT * FROM testdb1.campaigns")

    cursor.execute(CREATE_AGGREGATED_STATS)
    cursor.execute(LOAD_AGGREGATED_STATS)
    conn.commit()
