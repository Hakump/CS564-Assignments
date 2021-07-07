create index cus_m_k on CUSTOMER(C_MKTSEGMENT, C_CUSTKEY);
create index lin_k_s on LINEITEM(L_ORDERKEY, L_SHIPDATE) where L_SHIPDATE > '1995-03-15' ;
create index ord_k_d_p on ORDERS(O_ORDERKEY,O_ORDERDATE,O_ORDERPRIORITY);

.timer on
EXPLAIN QUERY PLAN
select
	l_orderkey,
	sum(l_extendedprice*(1-l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer inner join ORDERS on c_mktsegment = 'BUILDING' and c_custkey = o_custkey,
	--orders,
	lineitem
where
-- 	c_mktsegment = 'BUILDING' --'[SEGMENT]'
-- 	and c_custkey = o_custkey
-- 	and
        l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15' --'[DATE]'
	and l_shipdate > '1995-03-15' --'[DATE]'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;

DROP INDEX cus_m_k;
DROP INDEX lin_k_s;
DROP INDEX ord_k_d_p;