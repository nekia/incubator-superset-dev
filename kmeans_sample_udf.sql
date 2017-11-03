-- DROP FUNCTION IF EXISTS kmeans_sample_udf( data_array_entry text[] );
DROP TABLE IF EXISTS km_sample CASCADE;
DROP TABLE IF EXISTS rettbl;


CREATE OR REPLACE FUNCTION kmeans_sample_udf(
    _tbl_type anyelement,
    data_array_entry text[],
    num_cluster integer,
    fn_dist text,
    agg_centroid text,
    max_num_iterations integer,
    min_frac_reassigned double precision )
RETURNS SETOF anyelement AS
$func$
DECLARE
    i text;
    entry text = '';
BEGIN

    DROP TABLE IF EXISTS km_sample_src;
    DROP TABLE IF EXISTS km_result;

    FOREACH i IN ARRAY data_array_entry
    LOOP
        entry := entry || i || ', ';
    END LOOP;
    entry := substring(entry from 1 for length(entry)-2 );
    raise notice '%', entry;

    EXECUTE format('CREATE TABLE km_sample_src AS
    SELECT b.pid, a.points_array as points from (
        SELECT km_sample.pid, ARRAY[ %1$s ] as points_array FROM km_sample
    ) a, km_sample b
    where a.pid = b.pid', entry);

    EXECUtE format('CREATE TABLE km_result AS
    SELECT * FROM madlib.kmeanspp(''km_sample_src'', ''points'', $1,
                            ''madlib.%s'',
                            ''madlib.%s'', $2, $3)', fn_dist, agg_centroid )
    USING num_cluster, max_num_iterations, min_frac_reassigned;

    -- RETURN QUERY EXECUTE format('
    EXECUTE format('
        INSERT INTO %I
        SELECT km_sample.pid, (madlib.closest_column(centroids, points)).column_id as cluster_id, %s
        FROM km_sample, km_sample_src, km_result
        WHERE km_sample.pid = km_sample_src.pid
        ORDER BY km_sample.pid', pg_typeof(_tbl_type), entry);
    RETURN QUERY EXECUTE format('TABLE %s', pg_typeof(_tbl_type));

END;
$func$ LANGUAGE plpgsql;

CREATE TABLE km_sample(pid int,
    points1 double precision,
    points2 double precision,
    points3 double precision,
    points4 double precision,
    points5 double precision,
    points6 double precision,
    points7 double precision,
    points8 double precision,
    points9 double precision,
    points10 double precision,
    points11 double precision,
    points12 double precision,
    points13 double precision,
    cluster_id integer
);

INSERT INTO km_sample VALUES
(1,  14.23, 1.71, 2.43, 15.6, 127, 2.8, 3.0600, 0.2800, 2.29, 5.64, 1.04, 3.92, 1065, 0),
(2,  13.2, 1.78, 2.14, 11.2, 1, 2.65, 2.76, 0.26, 1.28, 4.38, 1.05, 3.49, 1050, 0),
(3,  13.16, 2.36,  2.67, 18.6, 101, 2.8,  3.24, 0.3, 2.81, 5.6799, 1.03, 3.17, 1185, 0),
(4,  14.37, 1.95, 2.5, 16.8, 113, 3.85, 3.49, 0.24, 2.18, 7.8, 0.86, 3.45, 1480, 0),
(5,  13.24, 2.59, 2.87, 21, 118, 2.8, 2.69, 0.39, 1.82, 4.32, 1.04, 2.93, 735, 0),
(6,  14.2, 1.76, 2.45, 15.2, 112, 3.27, 3.39, 0.34, 1.97, 6.75, 1.05, 2.85, 1450, 0),
(7,  14.39, 1.87, 2.45, 14.6, 96, 2.5, 2.52, 0.3, 1.98, 5.25, 1.02, 3.58, 1290, 0),
(8,  14.06, 2.15, 2.61, 17.6, 121, 2.6, 2.51, 0.31, 1.25, 5.05, 1.06, 3.58, 1295, 0),
(9,  14.83, 1.64, 2.17, 14, 97, 2.8, 2.98, 0.29, 1.98, 5.2, 1.08, 2.85, 1045, 0),
(10, 13.86, 1.35, 2.27, 16, 98, 2.98, 3.15, 0.22, 1.8500, 7.2199, 1.01, 3.55, 1045, 0);

CREATE TABLE rettbl(pid int,
    points1 double precision,
    points2 double precision,
    points3 double precision,
    cluster_id integer
);

-- create table rettbl as
select * from kmeans_sample_udf( NULL::public.kmeans_sample_udf_ret, ARRAY[
        'points1',
        'points2',
        'points3'
        -- 'points4',
        -- 'points5',
        -- 'points6',
        -- 'points7',
        -- 'points8',
        -- 'points9',
        -- 'points10',
        -- 'points11',
        -- 'points12',
        -- 'points13'
    ], 4, 'squared_dist_norm2', 'avg', 20, 0.001 );
