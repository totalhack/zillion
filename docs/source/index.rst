Zillion: Make sense of it all
=============================

``Zillion`` is a free, open data warehousing and dimensional modeling tool
that allows combining and analyzing data from multiple datasources through a
simple API. It writes SQL so you don't have to, and it easily bolts onto
existing database infrastructure that can be understood by SQLALchemy.

With ``Zillion`` you can:

- Define a warehouse that contains a variety of SQL and/or file-like datasources
  - Define or reflect metrics and dimensions in your data
  - Establish table relationships in a star or snowflake schema
  - Define formulas at the datasource level or based on combinations of metrics and dimensions
- Query multiple datasources at once and combine the results in a simple result DataFrame
- Flexibly aggregate your data with multi-level rollups and table pivots
- Apply technical transformations on columns including rolling, cumulative, and rank statistics
- Save and load report specifications
- Optional automatic type conversions - i.e. get a "year" dimension for free from a "date" column
- Utilize "adhoc" datasources and fields to enrich specific report requests

.. toctree::
   :maxdepth: 3

   Quickstart <quickstart>
   Zillion Reference <zillion>
