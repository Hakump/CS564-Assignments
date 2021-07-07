create index price_part on PARTSUPP(PS_PARTKEY, PS_SUPPLYCOST);
create index part_search on PART(P_SIZE, P_TYPE);
-- create index supply_k_n on SUPPLIER(S_SUPPKEY, S_NATIONKEY);
-- create index na_k_r on NATION(N_NATIONKEY, N_REGIONKEY);
-- create index region_k_name on REGION(R_REGIONKEY, R_NAME) where R_NAME = 'ASIA';

.timer on
EXPLAIN QUERY PLAN
SELECT
	s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment,ps_supplycost
FROM
	part, supplier, partsupp, nation, region
WHERE
	p_partkey = ps_partkey
	AND s_suppkey = ps_suppkey
	AND p_size = 11 -- [SIZE]
	AND p_type like 'MEDIUM BRUSHED COPPER' -- '%[TYPE]'
	AND s_nationkey = n_nationkey
	AND n_regionkey = r_regionkey
	AND r_name = 'ASIA' -- '[REGION]'
	AND ps_supplycost = (
		SELECT
			min(ps_supplycost)
		FROM
			partsupp, supplier, nation, region
		WHERE
			p_partkey = ps_partkey
			AND s_suppkey = ps_suppkey
			AND s_nationkey = n_nationkey
			AND n_regionkey = r_regionkey
			AND r_name = 'ASIA' -- '[REGION]'
		)
ORDER BY
	s_acctbal DESC,
	n_name,
	s_name,
	p_partkey;

DROP INDEX price_part;
DROP INDEX part_search;