class SqlQueries:
    state_table_insert = ("""
        SELECT
                dem.id_state,
                dem.name_state,
                dem.american_native ,
                dem.asian ,
                dem.african_american ,
                dem.hispanic ,
                dem.white ,
                dem.total_population,
                temp.avg_temp ,
                temp.autumn ,
                temp.spring ,
                temp.summer ,
                temp.winter 
            FROM staging_demographic dem
            JOIN staging_temperature temp
            ON dem.id_state = temp.id_state
    """)

    education_table_insert = ("""
        SELECT distinct id_institution, institution_name, city, id_state,  n_students, n_employees, availability
        FROM staging_education
    """)
    
    covid_table_insert = ("""
        SELECT id_covid_data, day_date, id_state, positive, hospitalized_curr, hospitalized_acum, death
        FROM staging_covid
    """)
    