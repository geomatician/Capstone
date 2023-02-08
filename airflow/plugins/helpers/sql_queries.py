class SqlQueries:
    fact_table_insert = ("""
        SELECT
            i.cicid,
            t.city,
            a.code
        FROM
            public.immigration i
        INNER JOIN public.airportcode a 
            ON i.i94port = a.code
        INNER JOIN public.temperature t 
            ON a.city = t.city
    """)