DROP TABLE IF EXISTS public.fact;
DROP TABLE IF EXISTS public.immigration;
DROP TABLE IF EXISTS public.temperature;
DROP TABLE IF EXISTS public.airportcode;

CREATE TABLE IF NOT EXISTS public.fact (
	factid integer IDENTITY(1,1),
	cicid integer NOT NULL,
	city varchar(50) NOT NULL,
    code varchar(3) NOT NULL,
	CONSTRAINT fact_pkey PRIMARY KEY (factid)
);

CREATE TABLE IF NOT EXISTS public.immigration (
	cicid integer NOT NULL,
    i94port varchar(10),
    visatype varchar(10), 
    CONSTRAINT immi_pkey PRIMARY KEY (cicid)
);

CREATE TABLE IF NOT EXISTS public.temperature (
	city varchar(50) NOT NULL,
	min_temp DOUBLE PRECISION NOT NULL,
	avg_temp DOUBLE PRECISION NOT NULL,
	max_temp DOUBLE PRECISION NOT NULL,
	CONSTRAINT temp_pkey PRIMARY KEY (city)
);

CREATE TABLE IF NOT EXISTS public.airportcode (
	code varchar(3) NOT NULL,
	city varchar(50),
	statename varchar(50),
	CONSTRAINT codes_pkey PRIMARY KEY (code)
);







