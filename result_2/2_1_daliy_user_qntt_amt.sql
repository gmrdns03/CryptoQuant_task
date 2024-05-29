-- 특정 계정의 일별 보유량을 확인할 수 있는 시계열 데이터
select 
	COALESCE(t.addr, t.addr, f.addr) as addr,
	COALESCE(t.dt, t.dt, f.dt) as dt,
	COALESCE(t.acct, t.acct, 0) - COALESCE(f.sent, f.sent, 0) as qntt_amt
from
	(select 
		A.to_address as addr,
		TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
		SUM(A.amount) as acct
	from onchain_trans_log A
	where 
		A.to_address = '0x28c6c06298d514db089934071355e5743bf21d60'
		and A.token_address = '0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74'
		and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '2023-10-25' and '2024-03-30'
	group by A.to_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD')) t
	full outer join
	(select 
		A.from_address as addr,
		TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') as dt,
		SUM(A.amount) as sent
	from onchain_trans_log A
	where 
		A.from_address = '0x28c6c06298d514db089934071355e5743bf21d60'
		and A.token_address = '0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74'
		and TO_CHAR(A.block_timestamp, 'YYYY-MM-DD') between '2023-10-25' and '2024-03-30'
	group by A.from_address, TO_CHAR(A.block_timestamp, 'YYYY-MM-DD')) f
	on t.dt = f.dt
;