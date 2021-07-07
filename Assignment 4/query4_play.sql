create index Lin_k_c_r on LINEITEM(L_ORDERKEY, l_commitdate < l_receiptdate) where l_commitdate < l_receiptdate;
create index ord_k_d_d on ORDERS(O_ORDERKEY,O_ORDERPRIORITY)where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01';

create index Ord_O_O_ONE on ORDERS(O_ORDERPRIORITY, O_ORDERKEY )where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01';

.timer on
EXPLAIN QUERY PLAN
select
	temp.o_orderpriority,
	count(*) as order_count
from
    ((select O_ORDERKEY, O_ORDERPRIORITY from orders where o_orderdate < '1993-10-01'
    and o_orderdate >= '1993-07-01' ) inner join (select L_ORDERKEY from LINEITEM where l_commitdate < l_receiptdate) on  O_ORDERKEY = L_ORDERKEY) as temp
group by
	temp.o_orderpriority;

drop index Lin_k_c_r;
-- drop index ord_k_d_d;
--
drop index Ord_O_O_ONE;