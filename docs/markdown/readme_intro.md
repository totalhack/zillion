**Introduction**
----------------

`Zillion` is a data modeling and analytics tool that allows combining and
analyzing data from multiple datasources through a simple API. It acts as a semantic layer
on top of your data, writes SQL so you don't have to, and easily bolts onto existing
database infrastructure via SQLAlchemy Core. `Zillion` also includes an agentic
question-answering workflow for planning and executing one or more reports from
natural language.

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
* Plan and answer warehouse questions with a natural language agent
