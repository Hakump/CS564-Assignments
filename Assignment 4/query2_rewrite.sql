with temp as (
    select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, min(PS_SUPPLYCOST)
    from
         NATION join REGION on N_REGIONKEY= R_REGIONKEY
        inner join SUPPLIER on NATION.N_NATIONKEY = SUPPLIER.S_NATIONKEY
         inner join (PARTSUPP indexed by sqlite_autoindex_PARTSUPP_1)
        on PS_SUPPKEY = S_SUPPKEY
        inner join PART on P_PARTKEY = PS_PARTKEY

    where R_NAME = 'ASIA' and p_type like 'MEDIUM BRUSHED COPPER' and p_size = 11
    group by P_PARTKEY
    ORDER BY
	s_acctbal DESC,
	n_name,
	s_name,
	p_partkey
    )
select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM
	temp


	    -- '[REGION]'

