/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.sharding;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class GeoFunctionTest extends ReadBaseTestCase {

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public GeoFunctionTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Test
    public void geomfromtextTest() throws Exception {
        String sql = "select GEOMFROMTEXT('POINT(1 1)') from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_GEOMFROMTEXT('POINT(1 1)') from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geomCollfromtextTest() throws Exception {
        String sql = "select ST_GeomCollFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))') from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void linefromtextTest() throws Exception {
        String sql = "select ST_LineFromText('LINESTRING(0 0, 10 10, 20 25, 50 60)') from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void mlinefromtextTest() throws Exception {
        String sql = "select MLineFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))') from " + baseOneTableName
            + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_MLineFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))') from " + baseOneTableName
                + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void mPointfromtextTest() throws Exception {
        String sql = "select MPointFromText('MULTIPOINT (1 1, 2 2, 3 3)') from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_MPointFromText('MULTIPOINT (1 1, 2 2, 3 3)') from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void mPolfromtextTest() throws Exception {
        String sql = "select MPolyFromText('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))') from "
            + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_MPolyFromText('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))') from "
                + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pointfromtextTest() throws Exception {
        String sql = "select ST_PointFromText('POINT(15 20)') from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void polyfromtextTest() throws Exception {
        String sql = "select ST_PolyFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))') from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void asTest() throws Exception {
        String sql = "select ST_AsText(ST_GeomFromText('POINT(1 -1)')), ST_AsWKB(ST_GeomFromText('POINT(1 -1)')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geomFromWKBTest() throws Exception {
        String sql = "select ST_GeomFromWKB(ST_AsWKB(ST_GeomFromText('POINT(1 -1)'))) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void constructTest() throws Exception {
        String sql = "select Point(1,2) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void dimensionTest() throws Exception {
        String sql = "select ST_Dimension(ST_GeomFromText('LineString(1 1,2 2)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void envolopeTest() throws Exception {
        String sql = "select ST_Envelope(ST_GeomFromText('LineString(1 1,1 2)')) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geotypeTest() throws Exception {
        String sql = "select ST_GeometryType(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isemptyTest() throws Exception {
        String sql = "select IsEmpty(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_IsEmpty(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void issimpleTest() throws Exception {
        String sql = "select IsSimple(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_IsSimple(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void sridTest() throws Exception {
        String sql = "select ST_SRID(ST_GeomFromText('POINT(1 1)')) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pointPropTest() throws Exception {
        String sql = "select ST_X(Point(56.7, 53.34)), ST_Y(Point(56.7, 53.34)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void endPointTest() throws Exception {
        String sql = "select ST_EndPoint(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isClosedTest() throws Exception {
        String sql = "select ST_IsClosed(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void glengthTest() throws Exception {
        String sql = "select GLength(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName + " limit 1";
        if (isMySQL80()) {
            sql = "select ST_Length(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName + " limit 1";
        }
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void numPointsTest() throws Exception {
        String sql = "select ST_NumPoints(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pointnTest() throws Exception {
        String sql = "select ST_PointN(ST_GeomFromText('LineString(1 1,2 2,3 3)'),2) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void startpointTest() throws Exception {
        String sql = "select ST_StartPoint(ST_GeomFromText('LineString(1 1,2 2,3 3)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void areaTest() throws Exception {
        String sql = "select ST_Area(ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void centroidTest() throws Exception {
        String sql = "select ST_Centroid(ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void exteriorRingTest() throws Exception {
        String sql = "select ST_ExteriorRing(ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void exteriornTest() throws Exception {
        String sql = "select ST_InteriorRingN(ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'), 1) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void numInteriorRingsTest() throws Exception {
        String sql = "select ST_NumInteriorRings(ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geoNTest() throws Exception {
        String sql =
            "select ST_GeometryN(ST_GeomFromText('GeometryCollection(Point(1 1),LineString(2 2, 3 3))'), 1) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void numgeoTest() throws Exception {
        String sql =
            "select ST_NumGeometries(ST_GeomFromText('GeometryCollection(Point(1 1),LineString(2 2, 3 3))')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bufferTest() throws Exception {
        String sql = "select ST_Buffer(ST_GeomFromText('POINT(0 0)'), 0) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void bufferSTest() throws Exception {
        String sql = "select ST_Buffer_Strategy('join_round', 10) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void convexHullTest() throws Exception {
        String sql = "select ST_ConvexHull(ST_GeomFromText('MULTIPOINT(5 0,25 0,15 10,15 25)')) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void diffTest() throws Exception {
        String sql = "select ST_Difference(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void intersectionTest() throws Exception {
        String sql =
            "select ST_Intersection(ST_GeomFromText('LineString(1 1, 3 3)'), ST_GeomFromText('LineString(1 3, 3 1)')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void sysdiffTest() throws Exception {
        String sql = "select ST_SymDifference(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void unionTest() throws Exception {
        String sql =
            "select ST_Union(ST_GeomFromText('LineString(1 1, 3 3)'), ST_GeomFromText('LineString(1 3, 3 1)')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distanceTest() throws Exception {
        String sql = "select ST_Distance(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void equalsTest() throws Exception {
        String sql = "select ST_Equals(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void withinTest() throws Exception {
        String sql =
            "select MBRWithin(ST_GeomFromText('Polygon((0 0,0 3,3 3,3 0,0 0))'), ST_GeomFromText('Polygon((0 0,0 5,5 5,5 0,0 0))')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void containsTest() throws Exception {
        String sql =
            "select MBRContains(ST_GeomFromText('Polygon((0 0,0 3,3 3,3 0,0 0))'), ST_GeomFromText('Point(1 1)')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void coversTest() throws Exception {
        String sql =
            "select MBRCovers(ST_GeomFromText('Polygon((0 0,0 3,3 3,3 0,0 0))'), ST_GeomFromText('Point(1 1)')) from "
                + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geohashTest() throws Exception {
        String sql = "select  ST_GeoHash(180,0,10), ST_GeoHash(-180,-90,15) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void latfromTest() throws Exception {
        String sql = "select  ST_LatFromGeoHash(ST_GeoHash(45,-20,10)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void longfromTest() throws Exception {
        String sql = "select  ST_LongFromGeoHash(ST_GeoHash(45,-20,10)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void pointfromTest() throws Exception {
        String sql = "select  ST_PointFromGeoHash(ST_GeoHash(45,-20,10),0) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geojsonTest() throws Exception {
        String sql = "select ST_AsGeoJSON(ST_GeomFromText('POINT(11.11111 12.22222)'),2) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void geofromjsonTest() throws Exception {
        String sql = "select ST_GeomFromGeoJSON('{ \"type\": \"Point\", \"coordinates\": [102.0, 0.0]}') from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void validateTest() throws Exception {
        String sql = "select  ST_Validate(ST_GeomFromText('LINESTRING(0 0, 1 1)')) from " + baseOneTableName
            + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void simplifyTest() throws Exception {
        String sql = "select  ST_Simplify(ST_GeomFromText('LINESTRING(0 0,0 1,1 1,1 2,2 2,2 3,3 3)'), 0.5) from "
            + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void makeeTest() throws Exception {
        String sql = "select ST_MakeEnvelope(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void isvalidTest() throws Exception {
        String sql = "select ST_IsValid(ST_GeomFromText('LINESTRING(0 0, 1 1)')) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void distancesTest() throws Exception {
        String sql = "select ST_Distance_Sphere(Point(1,1), Point(2,2)) from " + baseOneTableName + " limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
