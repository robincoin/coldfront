# coldfront
Plugins and components for a MySQL Heatwave-like secondary engine duckdb implementation.  Tested in the 8.4 branch of [https://github.com/mysql/mysql-server:8.4](https://github.com/mysql/mysql-server/tree/8.4)

# compile
place the rapid/ folder in the mysql-server/storage folder
compile the server as normal

# install
```
INSTALL PLUGIN rapid SONAME 'ha_rapid.so';
```
## LD_PRELOAD
On 64 bit intel/amd architectures it may be necessary to LD_PRELOAD the ha_rapid.so library because DuckDB is so large.  This problem does not appear on aarch64 machines like Apple Silicon.

# usage
## Load table into secondary engine
```
ALTER TABLE test.t1 SECONDARY_ENGINE=RAPID;
ALTER TABLE test.t1 SECONDARY_LOAD;
```
## run query in secondary engine
```
set secondary_engine_cost_threshold=0;

mysql> select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit
from lineorder
join dim_date   on lo_orderdatekey = d_datekey
join customer   on lo_custkey = c_customerkey
join supplier   on lo_suppkey = s_suppkey
join part       on lo_partkey = p_partkey
where c_region = 'AMERICA'
and s_region = 'AMERICA'
and (d_year = 1997 or d_year = 1998)
+--------+---------------+------------+-----------+
| d_year | s_nation      | p_category | profit    |
+--------+---------------+------------+-----------+
|   1997 | ARGENTINA     | MFGR#11    | 172637327 |
|   1997 | ARGENTINA     | MFGR#12    | 167477477 |
|   1997 | ARGENTINA     | MFGR#13    | 135636601 |
|   1997 | ARGENTINA     | MFGR#14    | 188382151 |
|   1997 | ARGENTINA     | MFGR#15    | 188464669 |
|   1997 | ARGENTINA     | MFGR#21    | 139354852 |
|   1997 | ARGENTINA     | MFGR#22    | 148447588 |
|   1997 | ARGENTINA     | MFGR#23    | 187164657 |
|   1997 | ARGENTINA     | MFGR#24    | 201087869 |
|   1997 | ARGENTINA     | MFGR#25    | 168198309 |
|   1997 | BRAZIL        | MFGR#11    | 156061743 |
|   1997 | BRAZIL        | MFGR#12    | 158581362 |
|   1997 | BRAZIL        | MFGR#13    | 159192864 |
|   1997 | BRAZIL        | MFGR#14    | 172253253 |
|   1997 | BRAZIL        | MFGR#15    | 146817387 |
|   1997 | BRAZIL        | MFGR#21    | 171909018 |
|   1997 | BRAZIL        | MFGR#22    | 178801489 |
|   1997 | BRAZIL        | MFGR#23    | 185435686 |
|   1997 | BRAZIL        | MFGR#24    | 211159756 |
|   1997 | BRAZIL        | MFGR#25    | 212804566 |
|   1997 | CANADA        | MFGR#11    | 154098266 |
|   1997 | CANADA        | MFGR#12    | 134896489 |
|   1997 | CANADA        | MFGR#13    | 126544141 |
|   1997 | CANADA        | MFGR#14    | 130514643 |
|   1997 | CANADA        | MFGR#15    | 218372366 |
|   1997 | CANADA        | MFGR#21    | 207626572 |
|   1997 | CANADA        | MFGR#22    | 137762487 |
|   1997 | CANADA        | MFGR#23    | 158071282 |
|   1997 | CANADA        | MFGR#24    | 146406782 |
|   1997 | CANADA        | MFGR#25    | 146969262 |
|   1997 | PERU          | MFGR#11    | 192213258 |
|   1997 | PERU          | MFGR#12    | 120487599 |
|   1997 | PERU          | MFGR#13    | 142427733 |
|   1997 | PERU          | MFGR#14    | 183805310 |
|   1997 | PERU          | MFGR#15    | 152721024 |
|   1997 | PERU          | MFGR#21    | 179353755 |
|   1997 | PERU          | MFGR#22    | 210263316 |
|   1997 | PERU          | MFGR#23    | 172462904 |
|   1997 | PERU          | MFGR#24    | 129010059 |
|   1997 | PERU          | MFGR#25    | 166042828 |
|   1997 | UNITED STATES | MFGR#11    | 177002431 |
|   1997 | UNITED STATES | MFGR#12    | 100346988 |
|   1997 | UNITED STATES | MFGR#13    | 159176418 |
|   1997 | UNITED STATES | MFGR#14    | 125006830 |
|   1997 | UNITED STATES | MFGR#15    | 159345652 |
|   1997 | UNITED STATES | MFGR#21    | 200500492 |
|   1997 | UNITED STATES | MFGR#22    | 203320597 |
|   1997 | UNITED STATES | MFGR#23    | 175805370 |
|   1997 | UNITED STATES | MFGR#24    | 176220191 |
|   1997 | UNITED STATES | MFGR#25    | 193225313 |
|   1998 | ARGENTINA     | MFGR#11    | 109303296 |
|   1998 | ARGENTINA     | MFGR#12    | 106717353 |
|   1998 | ARGENTINA     | MFGR#13    |  91189361 |
|   1998 | ARGENTINA     | MFGR#14    | 100243815 |
|   1998 | ARGENTINA     | MFGR#15    | 106273350 |
|   1998 | ARGENTINA     | MFGR#21    | 103775016 |
|   1998 | ARGENTINA     | MFGR#22    | 111187025 |
|   1998 | ARGENTINA     | MFGR#23    | 109874883 |
|   1998 | ARGENTINA     | MFGR#24    | 121922033 |
|   1998 | ARGENTINA     | MFGR#25    |  75733938 |
|   1998 | BRAZIL        | MFGR#11    |  90632716 |
|   1998 | BRAZIL        | MFGR#12    |  74444214 |
|   1998 | BRAZIL        | MFGR#13    |  90696405 |
|   1998 | BRAZIL        | MFGR#14    | 102005489 |
|   1998 | BRAZIL        | MFGR#15    | 129205380 |
|   1998 | BRAZIL        | MFGR#21    |  72768858 |
|   1998 | BRAZIL        | MFGR#22    | 109975169 |
|   1998 | BRAZIL        | MFGR#23    | 134476374 |
|   1998 | BRAZIL        | MFGR#24    |  86423354 |
|   1998 | BRAZIL        | MFGR#25    | 127602167 |
|   1998 | CANADA        | MFGR#11    | 101228531 |
|   1998 | CANADA        | MFGR#12    | 150340538 |
|   1998 | CANADA        | MFGR#13    |  81444256 |
|   1998 | CANADA        | MFGR#14    | 102323179 |
|   1998 | CANADA        | MFGR#15    |  85297369 |
|   1998 | CANADA        | MFGR#21    | 115694075 |
|   1998 | CANADA        | MFGR#22    |  84638643 |
|   1998 | CANADA        | MFGR#23    |  94602011 |
|   1998 | CANADA        | MFGR#24    | 154777167 |
|   1998 | CANADA        | MFGR#25    |  85212415 |
|   1998 | PERU          | MFGR#11    | 111878100 |
|   1998 | PERU          | MFGR#12    | 115433077 |
|   1998 | PERU          | MFGR#13    |  80603564 |
|   1998 | PERU          | MFGR#14    |  70751618 |
|   1998 | PERU          | MFGR#15    |  91429033 |
|   1998 | PERU          | MFGR#21    |  90325585 |
|   1998 | PERU          | MFGR#22    |  86747695 |
|   1998 | PERU          | MFGR#23    | 123578337 |
|   1998 | PERU          | MFGR#24    | 120146427 |
|   1998 | PERU          | MFGR#25    |  86683638 |
|   1998 | UNITED STATES | MFGR#11    |  88014525 |
|   1998 | UNITED STATES | MFGR#12    | 100746532 |
|   1998 | UNITED STATES | MFGR#13    |  94585665 |
|   1998 | UNITED STATES | MFGR#14    |  52147204 |
|   1998 | UNITED STATES | MFGR#15    |  72397928 |
|   1998 | UNITED STATES | MFGR#21    |  87300013 |
|   1998 | UNITED STATES | MFGR#22    |  86990972 |
|   1998 | UNITED STATES | MFGR#23    | 113579879 |
|   1998 | UNITED STATES | MFGR#24    | 116524594 |
|   1998 | UNITED STATES | MFGR#25    | 107782961 |
+--------+---------------+------------+-----------+
100 rows in set (0.03 sec)
```
## explain plan
```
+----+-------------+------------+------------+------+---------------+------+---------+------+--------+----------+----------------------------------------------------------------------------+
| id | select_type | table      | partitions | type | possible_keys | key  | key_len | ref  | rows   | filtered | Extra                                                                      |
+----+-------------+------------+------------+------+---------------+------+---------+------+--------+----------+----------------------------------------------------------------------------+
|  1 | SIMPLE      | customer   | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 894062 |    10.00 | Using where; Using temporary; Using filesort; Using secondary engine RAPID |
|  1 | SIMPLE      | lineorder2 | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 995121 |    10.00 | Using where; Using join buffer (hash join); Using secondary engine RAPID   |
|  1 | SIMPLE      | dim_date   | NULL       | ALL  | NULL          | NULL | NULL    | NULL |   2556 |     1.90 | Using where; Using join buffer (hash join); Using secondary engine RAPID   |
|  1 | SIMPLE      | supplier   | NULL       | ALL  | NULL          | NULL | NULL    | NULL |  59571 |     1.00 | Using where; Using join buffer (hash join); Using secondary engine RAPID   |
|  1 | SIMPLE      | part       | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 993201 |     1.90 | Using where; Using join buffer (hash join); Using secondary engine RAPID   |
+----+-------------+------------+------------+------+---------------+------+---------+------+--------+----------+----------------------------------------------------------------------------+
5 rows in set, 1 warning (0.01 sec)
```
