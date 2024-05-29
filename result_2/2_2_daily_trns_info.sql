select 
	trans.dt,
	trans.token_addr,
	trans.from_addr_cnt,
	trans.to_addr_cnt,
	trans.qntt_trns_amt,
	COALESCE(price.avg_price,price.avg_price,0) as avg_price,
	case
		when price.avg_price is not null then (trans.qntt_trns_amt * price.avg_price)
		else null
	end as trns_amt
from (
	select 
		 A.token_address as token_addr,
		 TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
		 count(distinct A.from_address) as from_addr_cnt,
		 count(distinct A.to_address) as to_addr_cnt,
		 sum(A.amount) as qntt_trns_amt
	from onchain_trans_log A
	where 
		A.token_address = '0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74'
		and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '2023-10-25' and '2024-03-30'
	group by A.token_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') ) trans
	left join (select 
					TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') as dt,
					avg(B.price) as avg_price
				from coin_price_hist B
				where 
					B.token_address = '0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74'
					and TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') between '2023-10-25' and '2024-03-30'
				group by TO_CHAR(B.event_timestamp, 'YYYY-MM-DD') ) price
	on trans.dt = price.dt
;