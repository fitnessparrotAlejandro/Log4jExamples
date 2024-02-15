select holding.deal_tracking_num,
       holding.tran_num,
       holding.ins_num,
       insn.name as ins_type,
       coname asco,
       holding.reference,
       holding.start_date,
       holding.maturity_date,
       holding.settle_date as settle_date,
       pro.pymt_date as payment_date,
       ext_bunit.short_name,
       mins.first_trade_date,
       mins.last_trade_date,
       mins.expiration_date,
       mins.first_delivery_date,
       mins.last_delivery_date,
       ccode.contract_code,
       ccode.contract_code.desc,
       mins.tick_size,
       mins.tick_value,
       unit.unit_label as unit,
       live_deal_count,
       res.reset_date,
       res.istart_date as rfis_date,
       res.riend_date,
       res.start_date as reset_contract_start,
       res.end_date,
       syt.name as settlement_type,
       par.reset_period,
       idx.index_id,
       idx.index_name,
       idx_mkt.index_id as market_index_id,
       icdx_mkt.index_name as market_index_name,
       ich_disc_idx.index_name as param_discount_index,
       idb_disc_mkt.index_name as market_discount_index,
       pro.rate_dtmn_date
from ab_tran holding
inner join (
    select distinct ab.ins_num
    from ab_tran ab 
    inner join ab_tran_event abe on ab.tran_num = abe.tran_num
    where ab.tran_status = 3
    and ab.tran_type = 0
    and ab.asset_type
    and abe.event_type = 1000
    and abe.event_date >= (select business_date from configuration)
) live_deals on holding.ins_num = live_deals.ins_num
inner join instruments insn on holding.ins_type = insn.id_number
inner join currency ccy on holding.currency = ccy.id_number
inner join party ext_bunit on holding.external_bunit = ext_bunit.party_id
inner join misc_ins mins on mins.ins_num = holding.ins_num
inner join idx_def idx on idx.index_id = par.proj_index
inner join ich unit unit on idx.unit = unit.unit_id
select abcount.ins_num, count(1) as live_deal_count
from ab_tran abcount
where abcount.tran_status = 3
and abcount.tran_type
and  abcount.asset_type = 2
group by abcount.ins_num
) deal_count on deal_count.ins_num = holding.ins_num
ight join (select reset.
          from reset inner join
                  (select ins_num, param_seq_num, max(reset_seq_num) as
                  reset_seq_num from reset where param_seq_num = 0 group by
          ins_num, param_seq_num) last_rest
          on (reset.ins_num = last_rest.ins_num and reset.param_seq_num =
          last_rest.param_seq_num and reset.reset_seq_num = last_rest.
          reset_seq_num)) res
on (holding.ins_num = res.ins_num)
inner join settle_type syt on (syt.id_number = par.settlement_type)
inner join (select pf.*
          from profile pf inner join
                  (select ins_num, param_seq_num, max(profile_seq_num) as
                  profile_seq_num from profile where param_seq_num = 0 group by
          ins_num, param_seq_num) last_profile
          on (pf.ins_num = last_profile.ins_num and pf.param_seq_num =
          last_profile.param_seq_num and pf.profile_seq_num = last_profile.
          profile_seq_num)) pro
on (holding.ins_num = pro.ins_num)
left join idx_def idx_mkt on (idx_mkt.index_id = mins.market_px_index_id and
ich _mkt.db_status = 1)
inner join idx_def idx_disc_idx on (idx_disc_idx.index_id = par.disc_index and
                                  )

left join idx_def idx_disc_mkt on (idx_disc_mkt.index_id = idx_mkt.disc_index and
idx_disc_mkt.db_status = 1)
WHERE holding.tran_status = 3
    AND holding.toolset IN (17, 19, 18, 7)
    AND holding.tran_type = 2
    AND holding.asset_type = 2
    AND idx.db_status = 1
    AND par.param_seq_num = 0
    AND (holding.maturity_date >= (SELECT business_date FROM configuration)
        OR mins.expiration_date >= (SELECT business_date FROM configuration))
ORDER BY insn.name, holding.contract_code, holding.expiration_date, holding.reference
	   
	   