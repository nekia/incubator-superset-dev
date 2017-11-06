-- DROP FUNCTION IF EXISTS kmeans_sample_udf( data_array_entry text[] );
DROP TABLE IF EXISTS arima_result;
DROP FUNCTION IF EXISTS arima_udf();

CREATE OR REPLACE FUNCTION arima_udf(
    _tbl_type anyelement,
    input_tbl text,
    timestamp_col text,
    timeseries_col text,
    steps_ahead integer)
RETURNS SETOF anyelement AS
$func$
DECLARE
    i text;
    entry text = '';
BEGIN

    raise notice '%', 'Hello';
    DROP TABLE IF EXISTS arima_udf_output;
    DROP TABLE IF EXISTS arima_udf_output_residual;
    DROP TABLE IF EXISTS arima_udf_output_summary;
    DROP TABLE IF EXISTS arima_udf_forecast_output;
    
    EXECUTE format('SELECT madlib.arima_train( %L,
                           ''arima_udf_output'',
                           %L,
                           %L)', input_tbl, timestamp_col, timeseries_col);
    EXECUTE format('SELECT madlib.arima_forecast( ''arima_udf_output'',
                              ''arima_udf_forecast_output'',
                              %s
                            )', steps_ahead);

    -- RETURN QUERY EXECUTE format('
    EXECUTE format('
        INSERT INTO %s
        select ''2012-10-01 0:0''::date + %s, %s, null from arima_beer
        union all
            select ''2012-10-01 0:0''::date + (steps_ahead + src.num)::integer,
                null, forecast_value
            from arima_beer_forecast_output,
                (select count(*) as num from arima_beer) src',
        pg_typeof(_tbl_type), timestamp_col, timeseries_col);
    RETURN QUERY EXECUTE format('TABLE %s', pg_typeof(_tbl_type));

END;
$func$ LANGUAGE plpgsql;

CREATE TABLE arima_result(
    time_id timestamp,
    timeseries_data double precision,
    forecast_data double precision
);

select * from arima_udf( NULL::public.arima_result, 'arima_beer', 'time_id', 'value', 10 );
