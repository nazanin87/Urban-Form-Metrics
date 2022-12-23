-- SQL code to generate urban form metrics for urban centers
-- Assumes that the GHSL Urban Centers database is loaded as a PostGreSQL/PostGIS table
-- This table is called urbancenters. The geometry column is called geom. Unique id is id_hdc_g0

-- The shorelines dataset should be loaded in a table called shoreline
-- The ghsl population raster (1k resolution, Mollweide (954009) projection) 
-- should be loaded in a table called ghs_pop_1k. Band 1 is assumed

-- Note: extracted from ghsluc_analysis.py and aggregate_pointmetrics.py

-- create function to avoid exceptions with edge cases when clipping rasters
CREATE FUNCTION safe_ST_Clip(rast raster, geom geometry)
    RETURNS raster AS
    $$ BEGIN
        RETURN ST_Clip($1, $2);
        EXCEPTION WHEN others THEN
            RAISE NOTICE 'TopologyException';
            RETURN ST_MakeEmptyRaster(rast);
    END; $$ LANGUAGE plpgsql;

-- (1) COMPACTNESS
-- as well as the compactness score, returns the geometry of the convex hull as ch_geom
DROP TABLE IF EXISTS tmp_convex_hull;
CREATE TABLE tmp_convex_hull AS
    SELECT id_hdc_g0, ch_geom, 
            ST_Area(ucgeom::geography) / ST_Area(ch_geom::geography) AS compactness
    FROM (
        SELECT id_hdc_g0, ST_Union(ucgeom) AS ucgeom, ST_Union(ch_geom) AS ch_geom
            FROM
                (SELECT u.id_hdc_g0, ST_Intersection(ST_MakeValid(u.geom), s.geom) AS ucgeom, -- need intersection bc urban center sometimes includes water
                    ST_Intersection(ST_ConvexHull(u.geom), s.geom) AS ch_geom
                FROM urbancenters u LEFT JOIN shoreline s
                ON (ST_Intersects(s.geom, u.geom))) t0
            GROUP BY id_hdc_g0) t1;

-- update a few places that are Null, because of inaccuracies in the shoreline (so the UC is all water)
UPDATE tmp_convex_hull c
        SET ch_geom = ST_ConvexHull(u.geom), 
            compactness = ST_Area(u.geom::geography) / ST_Area(ST_ConvexHull(u.geom)::geography)
        FROM urbancenters u
        WHERE c.ch_geom is Null AND u.id_hdc_g0 = c.id_hdc_g0;

UPDATE tmp_convex_hull SET compactness = 1 WHERE compactness>1;

-- (2) DENSITY GRADIENTS
-- Since we don't know the center, we compute density gradients for every center and take the max
-- Null values here are not identified bc the urban center only has 1 or 2 cells
-- Negative slopes are possible when the highest-density center does not have an identified slope 
-- (bc all other cells are the same distance away)
-- as well as the slope and intercept, returns the geometry of the implied center (dens_gradient_center_geom)
DROP TABLE IF EXISTS tmp_density_gradients;

CREATE TABLE tmp_density_gradients AS
WITH cell_pops AS      -- temporary table of raster pixels that intersect the urban centers
    (SELECT u.id_hdc_g0, t0.rid, t0.x, t0.y, t0.geom, t0.val FROM 
        (SELECT DISTINCT ON (rid, x, y, id_hdc_g0) id_hdc_g0, rid, (ST_PixelAsCentroids(rast)).* -- DISTINCT is in case multiple urban centers intersect one tile
            FROM urbancenters u,
                ghs_pop_1k r
        WHERE ST_Intersects(r.rast, ST_Transform(u.geom, 954009)) -- intersect to get the relevant raster tiles
        ) t0, urbancenters u
    WHERE ST_Intersects(t0.geom, ST_Transform(u.geom, 954009))
            AND t0.id_hdc_g0 = u.id_hdc_g0),  -- intersect again to get the relevant raster pixels
    
dist_matrix AS
    (SELECT t1.id_hdc_g0, t1.rid, t1.x, t1.y, LN(t2.val) AS logpop2, ST_Distance(t1.geom, t2.geom) AS dist
    FROM cell_pops t1, cell_pops t2
    WHERE t1.id_hdc_g0 = t2.id_hdc_g0
    AND ST_Distance(t1.geom, t2.geom)>0
    AND t1.val>0 AND t2.val>0),

all_density_gradients AS
    (SELECT id_hdc_g0, rid, x AS px, y AS py, -1 * regr_slope(logpop2, dist) density_gradient_slope,
            regr_intercept(logpop2, dist) density_gradient_intercept
        FROM dist_matrix
        GROUP BY id_hdc_g0, rid, x, y),

density_gradients AS   -- take largest gradient
    (SELECT DISTINCT ON (id_hdc_g0) * 
       FROM all_density_gradients d WHERE density_gradient_slope IS NOT Null
       ORDER BY id_hdc_g0, density_gradient_slope DESC)

SELECT id_hdc_g0, density_gradient_slope, density_gradient_intercept,
           geom AS dens_gradient_center_geom FROM
    (SELECT d.*, (ST_PixelAsCentroids(rast)).*
    FROM density_gradients d, ghs_pop_1k r 
    WHERE d.rid = r.rid) t0
    WHERE x=px AND y=py;

-- (3) WEIGHTED DENSITY

DROP TABLE IF EXISTS tmp_wt_density;
CREATE TABLE tmp_wt_density AS 
    SELECT id_hdc_g0, SUM(pop) AS ghs_pop, SUM(pop) / COUNT(*) AS ghs_density, 
            CASE WHEN SUM(pop)=0 THEN 0 
                 ELSE SUM(pop*pop)/Sum(pop) END AS ghs_wt_density 
    FROM 
        (SELECT id_hdc_g0, unnest(ST_DumpValues(Safe_ST_Clip(rast,  
            ST_MakeValid(ST_Transform(geom, 954009))))) AS pop 
        FROM urbancenters b, ghs_pop_1k as r
        WHERE r.rast&&ST_Transform(b.geom, 954009) -- needed because spatial index not used
        AND ST_Intersects(r.rast, ST_Transform(b.geom, 954009)))  t0
    WHERE pop IS NOT Null
    GROUP BY id_hdc_g0;

-- MERGE TOGETHER
DROP TABLE IF EXISTS urbancenters_new;
CREATE TABLE urbancenters_new AS
    SELECT * FROM urbancenters
    FULL OUTER JOIN tmp_convex_hull t1 USING (id_hdc_g0)
    FULL OUTER JOIN tmp_wt_density t2 USING (id_hdc_g0)
    FULL OUTER JOIN tmp_density_gradients t3 USING (id_hdc_g0);

DROP TABLE tmp_convex_hull;
DROP TABLE tmp_density_gradients;
DROP TABLE tmp_wt_density;
