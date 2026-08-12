----AMR_gibberish_em_a1add_velocity

WITH apid_scriptor_base AS ( SELECT thf.order_id
                                  , thf.web_order_id
                                  , thf.sales_org
                                  , LOWER(REPLACE(b2_nm, ' ', ''))   AS b2_nm
                                  , LOWER(SPLIT_PART(b2_em, '@', 1)) AS email_handle
                                  , SPLIT_PART(b2_em, '@', 2)        AS b2_em_dom
                                  -- Using Jaro-Winkler similarity to compare cleaned name with email handle
                                  , CASE WHEN b2_em_dom IN
                                              ('163.com', 'aol.com', 'gmail.com', 'hotmail.com', 'icloud.com',
                                               'mac.com', 'me.com', 'outlook.com', 'qq.com', 'yahoo.com')
                                             THEN 1
                                             ELSE 0
                                         END                         AS common_em_dom_ind
                                  , CASE WHEN LENGTH(REPLACE(b2_nm, ' ', '')) = 0 OR b2_em IS NULL
                                             THEN NULL
                                             ELSE JAROWINKLER_SIMILARITY(LOWER(REPLACE(b2_nm, ' ', '')),
                                                                         LOWER(SPLIT_PART(b2_em, '@', 1)))
                                         END                         AS nm2em_match_score
                                  , a1v.a1_add
                                  , a1v.apple_map_add
                                  , a1v.latest_a1_add
                                  , thf.prod_desc
                                  , thf.PAYMENT_TYPES
                                  , a1v.a1_add_hvp
                                  , a1v.apple_map_hvp
                                  , a1v.latest_a1_hvp
                                  , a1v.Add_exceed_ind_60d
                               FROM (Select * from gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_traderhunt_frame
                                            where sales_org IN ('3400', '5600')) thf
                               JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_shipto_add_velocity_vw a1v
                                   ON thf.web_order_id = a1v.web_order_id
                              WHERE 1 = 1
                                AND sales_district = 'IN02'
                                AND payment_types LIKE ANY ('AGCD%', 'APID%')
    Or b2_em_dom ilike any ('%.top%', '%.uk%','%.games%')
    or b2_em_dom in ('0htmail.com','521wanli.com','570mu.com','a21.hhhrsc.com','a67.zznoo.com','angoodrm.com',
                    'badck.com','bananailove.com','bmdde.com','bmdek.com','bvole.com','clwqkl.asia','cqukk.com','cwkuy.com','ddiuuu.com',
                    'ducwd.com','dwadc.com','foxxmial.com','gmx.com','guaikuan.com','haiyue111.com','hcddy.com'
                    ,'hnbad.com','hquihdj.com','htiwe9.com','hun.he.cn','hwbak.com','hwdch.com','isr.hk.cn','istnsddy.com','jaccb.com',
                    'kabdy.com','klsmwl.com','kuckb.com','kzuak.com','muddc.com','nbwak.com','ndcak.com','ohiouy.com','padxy.com','pddub.com',
                    'piqiu668.com','piqiu778.com','qianjieli.com','sdubw.com','skwck.com','stuonewin.com','stuwinwin.com','tikokk.com',
                    'tztrump.com','tzwanyu.com','usokwan.com','wbcah.com','wdckk.com','xs8857.com','yackk.com','yaohan7.com','yckad.com',
                    'yyikz.com','yzyxyc.com','zhouwei7.com','zjliping.com','zwtg7.com', 'hgfdd.com', 'ywkux.com', 'extmail.us'))

SELECT *
FROM apid_scriptor_base
 where nm2em_match_score <  80;
