class SqlQueries:
    create_all_talbes = ("""
        CREATE TABLE IF NOT EXISTS public.staging_i94immi(
            cicid int4,
            i94yr int4,
            i94mon int4,
            i94cit int4,
            i94res int4,
            i94port varchar(256),
            arrdate varchar(256),
            i94mode int4,
            i94addr varchar(256),
            depdate varchar(256),
            i94bir int4,
            i94visa int4,
            occup varchar(256),
            gender varchar(256),
            airline varchar(256),
            fltno varchar(256),
            visatype varchar(256)
        );
      
        
        CREATE TABLE IF NOT EXISTS public.dim_mode(
            mode_id int4 NOT NULL,
            mode_name varchar(256) NOT NULL,
            CONSTRAINT dim_mode_pkey PRIMARY KEY(mode_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_location(
            location_id int4 NOT NULL,
            location_name varchar(256) NOT NULL,
            CONSTRAINT dim_location_pkey PRIMARY KEY(location_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_port(
            port_code varchar(256) NOT NULL,
            port_name varchar(256) NOT NULL,
            CONSTRAINT dim_port_pkey PRIMARY KEY(port_code)
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_address(
            address_code varchar(256) NOT NULL,
            address_name varchar(256) NOT NULL,
            CONSTRAINT dim_address_pkey PRIMARY KEY(address_code)
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_visa(
            visa_id int4 NOT NULL,
            visa_category varchar(256) NOT NULL,
            CONSTRAINT  dim_visa_pkey PRIMARY KEY (visa_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_airline( 
            airline_name varchar(256),
            iata_designator varchar(256) NOT NULL,	   
            digit_code varchar(256),
            icao_designator varchar(256),
            country_or_territory varchar(256),
            CONSTRAINT dim_airline_pkey PRIMARY KEY(iata_designator)
            
        );
        
        CREATE TABLE IF NOT EXISTS public.dim_visitor(
            visitor_id int4 NOT NULL,
            age int4 NOT NULL,
            gender varchar(256),
            occup varchar(256),
            CONSTRAINT dim_visitor_pkey PRIMARY KEY (visitor_id)
        );
        
    
        CREATE TABLE IF NOT EXISTS public.dim_time(
            time_id int4 NOT NULL,
            time_year int4 NOT NULL,
            time_month int4 NOT NULL,
            time_day int4 NOT NULL,
            CONSTRAINT dim_time_pkey PRIMARY KEY(time_id)
        );
    
        CREATE TABLE IF NOT EXISTS public.fact_visit_event(
            event_id int IDENTITY NOT NULL,
            visitor_id int4 NOT NULL REFERENCES public.dim_visitor(visitor_id) ,
            cit_id int4 NOT NULL REFERENCES public.dim_location(location_id),
            res_id int4 NOT NULL REFERENCES public.dim_location(location_id),
            port_code varchar(256) NOT NULL REFERENCES public.dim_port(port_code),
            address_code varchar(256) NOT NULL REFERENCES public.dim_address(address_code),
            mode_id int4 NOT NULL REFERENCES public.dim_mode(mode_id),
            visa_id int4 NOT NULL REFERENCES public.dim_visa(visa_id),
            visatype varchar(256),
            fltno varchar(256),
            airline_iata varchar(256) REFERENCES public.dim_airline(iata_designator),
            arrdate_id int4 NOT NULL REFERENCES public.dim_time(time_id),
            depdate_id int4 NOT NULL REFERENCES public.dim_time(time_id),
            CONSTRAINT fact_visit_event_pkey PRIMARY KEY(event_id)
        );
        
        
        
    """)


    dim_visitor_insert = ("""
        SELECT DISTINCT cicid as visitor_id, 
               i94bir as age,
               gender,
               occup
        FROM public.staging_i94immi  
    """)


    dim_arrdate_time_insert = ("""
        SELECT DISTINCT cast(arrdate as int4) as time_id, 
               cast(SUBSTRING(arrdate, 1, 4) as int4) as time_year, 
               cast(SUBSTRING(arrdate, 5, 2) as int4) as time_month,
               cast(SUBSTRING(arrdate, 7, 2) as int4) as time_day
        FROM public.staging_i94immi   
    """)

    dim_depdate_time_insert = ("""
        SELECT DISTINCT cast(depdate as int4) as time_id, 
               cast(SUBSTRING(depdate, 1, 4) as int4) as time_year, 
               cast(SUBSTRING(depdate, 5, 2) as int4) as time_month,
               cast(SUBSTRING(depdate, 7, 2) as int4) as time_day
        FROM public.staging_i94immi    
    """)


    fact_visit_insert = ("""
        SELECT cicid as visitor_id,
               i94cit as cit_id,
               i94res as res_id,
               i94port as port_code,
               i94addr as address_code,
               i94mode as mode_id,
               i94visa as visa_id,
               visatype,
               fltno, 
               airline as airline_iata,
               cast(arrdate as int4) as arrdate_id,
               cast(depdate as int4) as depdate_id   
        FROM public.staging_i94immi
    """)

    drop_all_talbes = ("""
        DROP TABLE IF EXISTS 
            public.staging_i94immi,
            public.fact_visit_event, 
            public.dim_visitor,
            public.dim_visa, 
            public.dim_port,
            public.dim_location,
            public.dim_address, public.dim_airline, public.dim_mode,
            public.dim_time
    """)




