**Introduction**
----------------

`Zillion` is a free, open data warehousing and dimensional modeling tool that
allows combining and analyzing data from multiple datasources through a simple
API. It writes SQL so you don't have to, and it easily bolts onto existing
database infrastructure via SQLAlchemy.

With `Zillion` you can:

* Define a warehouse that contains a variety of SQL and/or file-like
  datasources
* Define or reflect metrics, dimensions, and relationships in your data
* Run multi-datasource reports and combine the results in a DataFrame
* Flexibly aggregate your data with multi-level rollups and table pivots
* Customize or combine fields with formulas
* Apply technical transformations including rolling, cumulative, and rank
  statistics
* Apply automatic type conversions - i.e. get a "year" dimension for free
  from a "date" column
* Save and share report specifications
* Utilize ad hoc or public datasources, tables, and fields to enrich reports
