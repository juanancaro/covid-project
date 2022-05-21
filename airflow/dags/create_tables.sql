DROP TABLE IF EXISTS public.staging_demographic;
DROP TABLE IF EXISTS public.staging_education;
DROP TABLE IF EXISTS public.staging_temperature;
DROP TABLE IF EXISTS public.staging_covid;
DROP TABLE IF EXISTS public.education;
DROP TABLE IF EXISTS public.covid;
DROP TABLE IF EXISTS public.state;

CREATE TABLE IF NOT EXISTS public.staging_demographic(
    id_state int8,
	name_state varchar(256),
    american_native numeric,
    asian numeric,
    african_american numeric,
    hispanic numeric,
    white numeric,
    total_population int8
);

CREATE TABLE IF NOT EXISTS public.staging_education(
    id_institution int8,
    institution_name varchar(256),
    city varchar(256),
    name_state varchar(256),
    n_students numeric,
    n_employees numeric,
    availability varchar(256),
	id_state int8
);

CREATE TABLE IF NOT EXISTS public.staging_temperature(
    id_state int8,
    name_state varchar(256),
    avg_temp numeric,
    autumn numeric,
    spring numeric,
    summer numeric ,
    winter numeric
);

CREATE TABLE IF NOT EXISTS public.staging_covid(
    id_covid_data int8,
    day_date date,
    name_state varchar(256),
    positive numeric,
    hospitalized_curr numeric,
    hospitalized_acum numeric,
    death numeric,
	id_state int8
);

CREATE TABLE IF NOT EXISTS public."state"(
    id_state int8,
    name_state varchar(256),
    american_native numeric,
    asian numeric,
    african_american numeric,
    hispanic numeric,
    white numeric,
    total_population int8,
    avg_temp numeric,
    avg_autumn_temp numeric,
    avg_spring_temp numeric,
    avg_summer_temp numeric ,
    avg_winter_temp numeric ,
    CONSTRAINT state_pkey PRIMARY KEY (id_state)
);

CREATE TABLE IF NOT EXISTS public.education(
    id_institution int8,
    institution_name varchar(256),
    city varchar(256),
    id_state int8,
    n_students numeric,
    n_employees numeric,
    availability varchar(256),
    CONSTRAINT education_pkey PRIMARY KEY (id_institution),
    CONSTRAINT education_fkey FOREIGN KEY (id_state) REFERENCES "state"(id_state)
);

CREATE TABLE IF NOT EXISTS public.covid(
    id_covid_data int8,
    day_date date,
    id_state int8,
    positive numeric,
    hospitalized_curr numeric,
    hospitalized_acum numeric,
    death numeric,
    CONSTRAINT covid_pkey PRIMARY KEY (id_covid_data),
    CONSTRAINT covid_fkey FOREIGN KEY (id_state) REFERENCES "state"(id_state)
);


