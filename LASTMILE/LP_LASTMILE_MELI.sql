--DRop tablE tEMP_45.LP_LM_DETAILS_MELI
CREATE TABLE TEMP_45.LP_LM_DETAILS_MELI AS (
                   SELECT SHP.*,
                  UPPER(dri.SHP_LG_DRIVER_FIRST_NAME) ||' '|| UPPER(dri.SHP_LG_DRIVER_LAST_NAME) AS DRIVER_NAME,
                                      dri.SHP_LG_DRIVER_ID AS DRIVER_ID,
                                      comp.shp_company_name AS MLP,
                                      dri.SHP_COMPANY_ID AS CODIGO_TRANSPORTADORA,
                                      dri.SHP_LG_DRIVER_IDENTIF_TYPE AS DOCUMENTO,
                                      dri.SHP_LG_DRIVER_IDENTIF_ID AS NUM_DOCUMENTO,
                                      dri.SHP_LG_DRIVER_STATUS AS DRIVER_STATUS,
                                      logs.SHP_LG_VEHICLE_PLATE_ID AS PLATE,
                                      shp_lg_vehicle_type as Tipo_vehiculo,
                                      lshp.shp_lg_facility_id,
                                      lshp.SHP_LG_CLUSTER_ID AS Cluster_ID
                                      ,row_number() OVER(PARTITION BY shipment_id,ORD_ORDER_ID ORDER BY (BPP_CASHOUT_USD) DESC) as Fcy_2
                  FROM TEMP_45.LP_LASTMILE_01_VIEW SHP
                     LEFT JOIN WHOWNER.BT_SHP_LG_SHIPMENTS lshp ON lshp.shp_shipment_id = shp.shipment_id
                     LEFT JOIN BT_SHP_LG_SHIPMENTS_ROUTES logs ON logs.shp_shipment_id = shp.shipment_id
                     LEFT JOIN WHOWNER.LK_SHP_COMPANIES comp ON comp.SHP_company_ID = logs.shp_company_ID
                     LEFT JOIN LK_SHP_LG_DRIVERS dri ON dri.shp_lg_driver_id=logs.shp_lg_driver_id
                     
                    WHERE 
                    source<>'Totals'
                    -- AND date_shipped BETWEEN current_date-5 AND current_date-1
                    AND date_shipped BETWEEN '2021-01-01' and current_date -1
                    AND c_carrier = 'MERCADO ENVIOS'
                    -- AND pais in ('MLM','MLA')
                )WITH DATA;
--DROP TABLE TEMP_45.LP_LM_TOTALS_MELI
CREATE TABLE TEMP_45.LP_LM_TOTALS_MELI AS (        
                        SELECT 
        date_shipped as "Date bpp Parameter"
        ,CAST('0' as VARCHAR(1)) as "Fraud?",' '	Substatus_Clasification
        ,'correo' "Culpability (std)"
				,1 Fcy
        ,Pais 
				,picking_type 
				,c_carrier 
				,date_shipped
				,CAST('1900-01-01' as DATE) as date_delivered
				,CAST(TotalGMV  AS DECIMAL (38,2)) AS TotalGMV 
				,Total_Shipments 
				,0 shipment_Id
        -- ,CAST(0 AS VARCHAR(100)) AS IMEI 
				,0 ORD_ORDER_ID
				,CAST('1900-01-01' as DATE) as date_bpp 
				,' ' as status 
				,' ' substatus_id
				,' ' as substatus 
				,0 as BPP_CX
				,0 as BPP_CASHOUT_USD 
				,0 as GMV
                ,' ' as city
				,CAST(0 as DECIMAL (19,6)) latitude
				,CAST(0 as DECIMAL (19,6)) longitude
				,CAST('0' as varchar(10)) as zip_code
				,' ' as addres_ref
				,SHP_LOGISTIC_CENTER_ID
				-- CAMPOS FRAUDE
				,' ' cus_nickname_buy
				,' ' cus_nickname_sel
				,' ' DOM_DOMAIN_ID
				,' ' VERTICAL
				,' ' Tipo_fraude
				,-1 has_pf_pnr_c_neto
				,-1 has_pf_pnr_c_bruto
				,-1 pnr_c_me_solved
				,-1 pnr_c_claim_solved
				,-1 has_pf_empty_box
				,-1 has_claim
				,-1 claims_PDD
				,' ' Causa_BPP
				,-1 has_bpp
				,-1 bpp_amt
				,-1 bpp_debt
				,' ' as claim_first_opened_date
				,-1 culpability_sel
				,-1 culpability_buy
				,' ' culpability
				,CAST(' ' as CHAR(50)) as SHP_ORIGIN
                ,CAST(' ' as CHAR(50)) as Push_Carrier
                ,0 as Tipo_Fallo
				,' ' driver
				,' ' SHP_LABEL_TRACKING_NUMBER
				,'0' bpp_budget
				,0 SHP_RECEIVER_ID
        ,0 GEO_RCV_ADDRESS_ID
        ,GEO_RCV_COUNTRY_NAME
        ,GEO_RCV_STATE_NAME
        ,GEO_RCV_CITY_NAME
        ,0 GEO_RCV_ZIP_CODE
        ,GEO_RCV_ZIP_CODE_SHORT
        ,' ' TMS_BUYER_INFO_ADDRESS_LINE
        ,' ' TMS_BUYER_INFO_ZIP_CODE
        ,' ' TMS_BUYER_INFO_NAME
        ,0 TMS_BUYER_INFO_ID
        ,' ' SHP_OUT_ADDED_BY_USER
        ,0 CW_GEO_RCV_LONGITUDE
        ,0 CW_GEO_RCV_LATITUDE
				,' ' F_Clasification
        ,'Totals' as Source
        ,DRIVER_NAME
        ,DRIVER_ID
        ,MLP
        ,CODIGO_TRANSPORTADORA
        ,CAST(' ' as VARCHAR(100)) as DOCUMENTO
        ,CAST(' ' as VARCHAR(100)) as NUM_DOCUMENTO
        ,DRIVER_STATUS
        ,PLATE
        ,Tipo_vehiculo
        ,shp_lg_facility_id
        ,Cluster_ID
        ,1 as Fcy_2
        FROM (
              SELECT 
                 shp.SIT_SITE_ID as Pais
                ,SHP.shp_picking_type_id as picking_type
                ,CASE
                    WHEN shp_carrier_ID_ajus='MELI LOGISTICS' THEN 'MERCADO ENVIOS'
                    WHEN shp_carrier_ID_ajus='REPROCESOS CARRITO' THEN 'MERCADO ENVIOS'
                    WHEN shp_carrier_ID_ajus= 'MERCADOENVIOS' THEN 'MERCADO ENVIOS'
                    ELSE shp_carrier_ID_ajus 
                    END as c_carrier
                ,shp.shp_date_shipped_id as date_shipped
                ,SHP_LOGISTIC_CENTER_ID
                -- ,SM.SHP_RECEIVER_ID
                --,SM.GEO_RCV_ZIP_CODE_SHORT
                ,CAST( '0' as DECIMAL(19,0)) GEO_RCV_ZIP_CODE_SHORT
                --,SM.GEO_RCV_CITY_NAME
                ,CAST(' ' as VARCHAR(50) ) GEO_RCV_CITY_NAME
                --,SM.GEO_RCV_STATE_NAME
                ,CAST(' ' as VARCHAR(50)) GEO_RCV_STATE_NAME
                --,SM.GEO_RCV_COUNTRY_NAME
                ,CAST(' ' as VARCHAR(25)) GEO_RCV_COUNTRY_NAME
                -- ,GEO.CW_GEO_RCV_LONGITUDE
                -- ,GEO.CW_GEO_RCV_LATITUDE
                -- campos logistics
                    
                   --,UPPER(dri.SHP_LG_DRIVER_FIRST_NAME) ||' '|| UPPER(dri.SHP_LG_DRIVER_LAST_NAME) AS DRIVER_NAME,
                      ,CAST( ' ' as VARCHAR(301)) DRIVER_NAME
                      --dri.SHP_LG_DRIVER_ID AS DRIVER_ID,
                      ,CAST (0 as DECIMAL(11,0)) DRIVER_ID
                      ,comp.shp_company_name AS MLP
                      -- ,CAST(' ' as VARCHAR(255)) MLP
                      -- dri.SHP_COMPANY_ID AS CODIGO_TRANSPORTADORA,
                      ,CAST (0 as DECIMAL(19,0) ) as CODIGO_TRANSPORTADORA
                      -- dri.SHP_LG_DRIVER_IDENTIF_TYPE AS DOCUMENTO,
                      -- dri.SHP_LG_DRIVER_IDENTIF_ID AS NUM_DOCUMENTO,
                      -- dri.SHP_LG_DRIVER_STATUS AS DRIVER_STATUS,
                      ,CAST (' ' as VARCHAR(45))DRIVER_STATUS
                      --logs.SHP_LG_VEHICLE_PLATE_ID AS PLATE,
                      ,CAST (' ' as VARCHAR(45)) as PLATE
                      -- shp_lg_vehicle_type as Tipo_ve�culo,
                      ,CAST(' ' as VARCHAR(60)) as Tipo_vehiculo
                      ,lshp.shp_lg_facility_id
                      --lshp.SHP_LG_CLUSTER_ID AS Cluster_ID
                      ,CAST(' ' as VARCHAR(90)) Cluster_ID
                   ,sum(shp.SHP_ORDER_COST_USD) TotalGMV
                  ,count(distinct shp.shp_shipment_id) as Total_Shipments
                
                FROM WHOWNER.BT_SHP_SHIPMENTS shp
                LEFT OUTER JOIN WHOWNER.LK_SHP_SHIPPING_SERVICES SERV ON (SERV.shp_service_id=SHP.shp_service_id)
                LEFT JOIN WHOWNER.LK_SHP_ADDRESS shp_a on (shp.shp_receiver_address=shp_a.shp_add_id)
                LEFT JOIN SHIPMENT.BT_SHP_SHIPMENTS_SUMMARY SM ON (SM.SHP_SHIPMENT_ID = SHP.SHP_SHIPMENT_ID)
                -- LEFT JOIN TEMP_45.LP_LK_GEOPUNTOS AS GEO ON (SM.SHP_RECEIVER_ID=GEO.SHP_RECEIVER_ID)
                LEFT JOIN WHOWNER.BT_SHP_LG_SHIPMENTS lshp ON lshp.shp_shipment_id = shp.shp_shipment_id
                LEFT JOIN BT_SHP_LG_SHIPMENTS_ROUTES logs ON logs.shp_shipment_id = shp.shp_shipment_id
                LEFT JOIN WHOWNER.LK_SHP_COMPANIES comp ON comp.SHP_company_ID = logs.shp_company_ID
                -- LEFT JOIN LK_SHP_LG_DRIVERS dri ON dri.shp_lg_driver_id=logs.shp_lg_driver_id

                WHERE -- shp.shp_date_shipped_id BETWEEN current_date-5 AND current_date-1
                 shp.shp_date_shipped_id BETWEEN '2021-01-01' and current_date -1
				        AND shp.SHP_SOURCE_ID = 'MELI'
                AND shp.shp_shipping_mode_id = 'me2'
                AND shp.shp_type = 'forward'
                AND shp.shp_picking_type_id not in ('self_service', 'default')
                AND c_carrier = 'MERCADO ENVIOS'
                -- AND pais in ('MLM','MLA')
                -- AND GEO_RCV_ZIP_CODE_SHORT='1712'
                -- AND GEO_RCV_CITY_NAME='CASTELAR'
                -- AND shp.shp_picking_type_id = 'fulfillment'

                group by 1,2,3,4,5,6,7,8,9
                ,10,11,12,13,14,15,16,17,18) a
                  ) WITH DATA;

-- UNION DE LAS TABLAS

CREATE TABLE temp_45.lp_lastmile_MELI_step_01 as (
SEL 
"Date bpp Parameter","Fraud?",Substatus_Clasification,"Culpability (std)",Fcy,Pais,picking_type,c_carrier,date_shipped,date_delivered,Total_Shipments,shipment_Id,ORD_ORDER_ID,date_bpp,
status,substatus_id,substatus,BPP_CX,BPP_CASHOUT_USD,GMV,city,latitude,longitude,zip_code,addres_ref,SHP_LOGISTIC_CENTER_ID,cus_nickname_buy,cus_nickname_sel,DOM_DOMAIN_ID,VERTICAL,
Tipo_fraude,has_pf_pnr_c_neto,has_pf_pnr_c_bruto,pnr_c_me_solved,pnr_c_claim_solved,has_pf_empty_box,has_claim,claims_PDD,Causa_BPP,has_bpp,bpp_amt,bpp_debt,claim_first_opened_date,
culpability_sel,culpability_buy,culpability,SHP_ORIGIN,Push_Carrier,Tipo_Fallo,driver,SHP_LABEL_TRACKING_NUMBER,BPP_BUDGET,SHP_RECEIVER_ID,GEO_RCV_ADDRESS_ID,GEO_RCV_COUNTRY_NAME,
GEO_RCV_STATE_NAME,GEO_RCV_CITY_NAME,GEO_RCV_ZIP_CODE,GEO_RCV_ZIP_CODE_SHORT,TMS_BUYER_INFO_ADDRESS_LINE,TMS_BUYER_INFO_ZIP_CODE,TMS_BUYER_INFO_NAME,TMS_BUYER_INFO_ID,SHP_OUT_ADDED_BY_USER,
CW_GEO_RCV_LONGITUDE,CW_GEO_RCV_LATITUDE,F_Clasification,Source,TotalGMV,DRIVER_NAME,DRIVER_ID,MLP,CODIGO_TRANSPORTADORA,DOCUMENTO,NUM_DOCUMENTO,DRIVER_STATUS,PLATE,Tipo_vehiculo,
SHP_LG_FACILITY_ID,Cluster_ID,Fcy_2
FROM TEMP_45.LP_LM_DETAILS_MELI

UNION

SEL "Date bpp Parameter","Fraud?",Substatus_Clasification,"Culpability (std)",Fcy,Pais,picking_type,c_carrier,date_shipped,date_delivered,Total_Shipments,shipment_Id,ORD_ORDER_ID,date_bpp,
status,substatus_id,substatus,BPP_CX,BPP_CASHOUT_USD,GMV,city,latitude,longitude,zip_code,addres_ref,SHP_LOGISTIC_CENTER_ID,cus_nickname_buy,cus_nickname_sel,DOM_DOMAIN_ID,VERTICAL,
Tipo_fraude,has_pf_pnr_c_neto,has_pf_pnr_c_bruto,pnr_c_me_solved,pnr_c_claim_solved,has_pf_empty_box,has_claim,claims_PDD,Causa_BPP,has_bpp,bpp_amt,bpp_debt,claim_first_opened_date,
culpability_sel,culpability_buy,culpability,SHP_ORIGIN,Push_Carrier,Tipo_Fallo,driver,SHP_LABEL_TRACKING_NUMBER,BPP_BUDGET,SHP_RECEIVER_ID,GEO_RCV_ADDRESS_ID,GEO_RCV_COUNTRY_NAME,
GEO_RCV_STATE_NAME,GEO_RCV_CITY_NAME,GEO_RCV_ZIP_CODE,GEO_RCV_ZIP_CODE_SHORT,TMS_BUYER_INFO_ADDRESS_LINE,TMS_BUYER_INFO_ZIP_CODE,TMS_BUYER_INFO_NAME,TMS_BUYER_INFO_ID,SHP_OUT_ADDED_BY_USER,
CW_GEO_RCV_LONGITUDE,CW_GEO_RCV_LATITUDE,F_Clasification,Source,TotalGMV,DRIVER_NAME,DRIVER_ID,MLP,CODIGO_TRANSPORTADORA,DOCUMENTO,NUM_DOCUMENTO,DRIVER_STATUS,PLATE,Tipo_vehiculo,
SHP_LG_FACILITY_ID,Cluster_ID,Fcy_2
FROM TEMP_45.LP_LM_TOTALS_MELI
) WITH DATA;

DROP TABLE TEMP_45.LP_LASTMILE_MELI;
CREATE TABLE TEMP_45.LP_LASTMILE_MELI as (

SELECT lm.*
,CASE shp_lg_facility_id
WHEN 'SAM1' THEN 'Norte'
WHEN 'SBA1' THEN 'Norte'
WHEN 'SBA2' THEN 'Norte'
WHEN 'SJP1' THEN 'Norte'
WHEN 'SRJ6' THEN 'Rio de Janeiro'
WHEN 'SRJ7' THEN 'Rio de Janeiro'
WHEN 'SRJ5' THEN 'Rio de Janeiro'
WHEN 'SRJ4' THEN 'Rio de Janeiro'
WHEN 'SRJ3' THEN 'Rio de Janeiro'
WHEN 'SC_VG' THEN 'SP Capital'
WHEN 'SC_ZS' THEN 'SP Capital'
WHEN 'SCE1' THEN 'Norte'
WHEN 'SDF1' THEN 'Minas Gerais'
WHEN 'SES1' THEN 'Rio de Janeiro'
WHEN 'SGO1' THEN 'Minas Gerais'
WHEN 'SMG1' THEN 'Minas Gerais'
WHEN 'SPE1' THEN 'Norte'
WHEN 'SPR1' THEN 'Sul'
WHEN 'SRJ1' THEN 'Rio de Janeiro'
WHEN 'SRJ2' THEN 'Rio de Janeiro'
WHEN 'SRS1' THEN 'Sul'
WHEN 'SSP10' THEN 'SP Interior'
WHEN 'SSP11' THEN 'SP Interior'
WHEN 'SSP12' THEN 'SP Interior'
WHEN 'SSP13' THEN 'SP Interior'
WHEN 'SSP14' THEN 'SP Interior'
WHEN 'SSP15' THEN 'SP Capital'
WHEN 'SSP16' THEN 'SP Capital'
WHEN 'SSP17' THEN 'SP Capital'
WHEN 'SSP3' THEN 'SP Interior'
WHEN 'SSP4' THEN 'SP Interior'
WHEN 'SSP5' THEN 'SP Capital'
WHEN 'SSP6' THEN 'SP Capital'
WHEN 'SSP7' THEN 'SP Capital'
WHEN 'SSP8' THEN 'SP Capital'
WHEN 'SSP9' THEN 'SP Interior'
WHEN 'SSC1' THEN 'Sul'
WHEN 'SRS5' THEN 'Sul'
WHEN 'SRJ7' THEN 'Rio de Janeiro'
WHEN 'SRJ8' THEN 'Rio de Janeiro'
WHEN 'SRN1' THEN 'Norte'
WHEN 'SSP18' THEN 'SP Interior'
WHEN 'SMG2' THEN 'Minas Gerais'
WHEN 'SSP19' THEN 'SP Capital'
WHEN 'SSP20' THEN 'SP Interior'
WHEN 'SSP21' THEN 'SP Capital'
WHEN 'SSC2' THEN 'Sul'
WHEN 'SSC3' THEN 'Sul'
WHEN 'SSC4' THEN 'Sul'
WHEN 'SMG3' THEN 'Minas Gerais'
WHEN 'SSE1' THEN 'Norte'
WHEN 'SMG4' THEN 'Minas Gerais'
WHEN 'SMG5' THEN 'Minas Gerais'
WHEN 'SMG6' THEN 'Minas Gerais'
WHEN 'SPR2' THEN 'Sul'
WHEN 'SPR3' THEN 'Sul'
WHEN 'SMR1' THEN 'Minas Gerais'
WHEN 'SBU1' THEN 'Buenos Aires'
WHEN 'SBU2' THEN 'Buenos Aires'
WHEN 'SBU3' THEN 'Buenos Aires'
WHEN 'SCF2' THEN 'Buenos Aires'
WHEN 'SCO1' THEN 'Interior'
WHEN 'SME1' THEN 'Interior'
WHEN 'SLA1' THEN 'Buenos Aires'
WHEN 'SRO1' THEN 'Interior'
WHEN 'SCF1' THEN 'Buenos Aires'
WHEN 'COCU01' THEN 'Bogota'
WHEN 'COXBG1' THEN 'Bogota'
WHEN 'SAN1' THEN 'Medellin'
WHEN 'SVA1' THEN 'Cali'
WHEN 'SBO1' THEN 'Bogota'
WHEN 'SRS4' THEN 'Sul'
WHEN 'SRJ10' THEN 'Rio de Janeiro'
WHEN 'SSP22' THEN 'SP Interior'
WHEN 'SMG8' THEN 'Minas Gerais'
WHEN 'SMN1' THEN 'Norte'
WHEN 'SPA1' THEN 'Norte'
WHEN 'SRJ9' THEN 'Rio de Janeiro'
WHEN 'SMG7' THEN 'Minas Gerais'
WHEN 'SRS2' THEN 'Sul'
WHEN 'SPI1' THEN 'Norte'
WHEN 'SRS3' THEN 'Sul'
WHEN 'SBA3' THEN 'Norte'
WHEN 'SRS5' THEN 'Sul'
WHEN 'SBA2' THEN 'Norte'
WHEN 'SRJ6' THEN 'Rio de Janeiro'
WHEN 'SAL1' THEN 'Norte'
WHEN 'SPR4' THEN 'Sul'
WHEN 'SRS6' THEN 'Sul'
WHEN 'SRS7' THEN 'Sul'
WHEN 'SMX1' THEN 'Metro Norte'
WHEN 'SMX5' THEN 'Metro Norte'
WHEN 'SMX2' THEN 'Metro Norte'
WHEN 'SHP1' THEN 'Metro Norte'
WHEN 'STL1' THEN 'Metro Norte'
WHEN 'SMX6' THEN 'Metro Norte'
WHEN 'SMX3' THEN 'Metro Sur'
WHEN 'SMX4' THEN 'Metro Sur'
WHEN 'SMX7' THEN 'Metro Sur'
WHEN 'SCV1' THEN 'Metro Sur'
WHEN 'SGR1' THEN 'Metro Sur'
WHEN 'SPZ1' THEN 'Golfo'
WHEN 'SPB1' THEN 'Golfo'
WHEN 'SVR1' THEN 'Golfo'
WHEN 'SJA1' THEN 'Golfo'
WHEN 'SOX1' THEN 'Golfo'
WHEN 'SDC1' THEN 'Golfo'
WHEN 'SPL1' THEN 'Golfo'
WHEN 'SCN1' THEN 'Sureste'
WHEN 'SQR1' THEN 'Bajio'
WHEN 'SMD1' THEN 'Sureste'
WHEN 'SCP1' THEN 'Sureste'
WHEN 'SVH1' THEN 'Sureste'
WHEN 'STG1' THEN 'Sureste'
WHEN 'STP1' THEN 'Sureste'
WHEN 'SMN1' THEN 'Sureste'
WHEN 'SMT1' THEN 'Noreste'
WHEN 'SMT2' THEN 'Noreste'
WHEN 'STA1' THEN 'Noreste'
WHEN 'SMA1' THEN 'Noreste'
WHEN 'SNL1' THEN 'Noreste'
WHEN 'SVM1' THEN 'Noreste'
WHEN 'SRX1' THEN 'Noreste'
WHEN 'SLW1' THEN 'Noreste'
WHEN 'SCH1' THEN 'Noroeste'
WHEN 'SCJ1' THEN 'Noroeste'
WHEN 'STR1' THEN 'Noroeste'
WHEN 'SPD1' THEN 'Noroeste'
WHEN 'SDG1' THEN 'Noroeste'
WHEN 'SZC1' THEN 'Noroeste'
WHEN 'SCOA' THEN 'Noroeste'
WHEN 'STJ1' THEN 'Norte-Pacifico'
WHEN 'SXL1' THEN 'Norte-Pacifico'
WHEN 'SHM1' THEN 'Norte-Pacifico'
WHEN 'SCE1' THEN 'Norte-Pacifico'
WHEN 'SLP1' THEN 'Norte-Pacifico'
WHEN 'SJD1' THEN 'Norte-Pacifico'
WHEN 'SCU1' THEN 'Norte-Pacifico'
WHEN 'SMO1' THEN 'Norte-Pacifico'
WHEN 'SGD1' THEN 'Centro'
WHEN 'SGD2' THEN 'Centro'
WHEN 'SCQ1' THEN 'Centro'
WHEN 'SZL1' THEN 'Centro'
WHEN 'SPV1' THEN 'Centro'
WHEN 'STN1' THEN 'Centro'
WHEN 'SMZ1' THEN 'Centro'
WHEN 'SAG1' THEN 'Bajio'
WHEN 'SLE1' THEN 'Bajio'
WHEN 'SCY1' THEN 'Bajio'
WHEN 'SSL1' THEN 'Bajio'
WHEN 'SBJ1' THEN 'Bajio'
WHEN 'SQR1' THEN 'Bajio'
WHEN 'SML1' THEN 'Bajio'
WHEN 'SLZ1' THEN 'Bajio'
WHEN 'SRI1' THEN 'Pereira'
WHEN 'SPR6' THEN 'Sul'
WHEN 'SLS1' THEN 'Norte'
WHEN 'SVP1' THEN 'Centro'
WHEN 'SRM1' THEN 'Centro'
WHEN 'SRM2' THEN 'Centro'
WHEN 'SRM3' THEN 'Centro'
WHEN 'STC1' THEN 'Sur'
WHEN 'SBB1' THEN 'Sur'
WHEN 'SRC1' THEN 'Centro'
WHEN 'SMI1' THEN 'Golfo'
WHEN 'SCO1' THEN 'Sureste'
WHEN 'SCT1' THEN 'Sureste'
WHEN 'SCP1' THEN 'Sureste'
WHEN 'SMQ1' THEN 'Interior'
WHEN 'SAF1' THEN 'Norte'
WHEN 'SBY1' THEN 'Tunja'
WHEN 'SMS1' THEN 'Minas Gerais'
WHEN 'SSP26' THEN 'SP Interior'
WHEN 'SSP29' THEN 'SP Interior'
WHEN 'STO1' THEN 'Norte'
WHEN 'SPR7' THEN 'Sul'
WHEN 'SMG12' THEN 'Minas Gerais'
WHEN 'SSP27' THEN 'SP Interior'
WHEN 'SGO2' THEN 'Minas Gerais'
WHEN 'SSC5' THEN 'Sul'
WHEN 'SMS2' THEN 'Minas Gerais'
WHEN 'SPR5' THEN 'Sul'
WHEN 'SRO1' THEN 'Norte'
WHEN 'SSP31' THEN 'SP Interior'
WHEN 'SSP25' THEN 'SP Interior'
WHEN 'SMG9' THEN 'Minas Gerais'
WHEN 'SRJ13' THEN 'Rio de Janeiro'
WHEN 'SSC7' THEN 'Sul'
WHEN 'SMG13' THEN 'Minas Gerais'
WHEN 'SAP1' THEN 'Norte'
WHEN 'SSC6' THEN 'Sul'
WHEN 'SSP24' THEN 'SP Interior'
WHEN 'SRS8' THEN 'Sul'
WHEN 'SMG10' THEN 'Minas Gerais'
WHEN 'SMG11' THEN 'Minas Gerais'
WHEN 'SRJ12' THEN 'Rio de Janeiro'
WHEN 'SES2' THEN 'Rio de Janeiro'
WHEN 'SBA4' THEN 'Norte'
WHEN 'SSP28' THEN 'SP Interior'
WHEN 'SRJ11' THEN 'Rio de Janeiro'
WHEN 'SRS9' THEN 'Sul'
WHEN 'SRS10' THEN 'Sul'
WHEN 'SSP30' THEN 'SP Interior'
WHEN 'SSP23' THEN 'SP Capital'
WHEN 'SPM1' THEN 'Sur'
WHEN 'SVP2' THEN 'Centro'
WHEN 'SSA1' THEN 'Bucaramanga'
WHEN 'SLV1' THEN 'Noroeste'
WHEN 'SNU1' THEN 'Sur'
WHEN 'SRM4' THEN 'Centro'
WHEN 'SVL1' THEN 'Sur'
WHEN 'SSF1' THEN 'Interior'
WHEN 'SCD1' THEN 'Norte-Pacifico'
WHEN 'STI1' THEN 'Ibague'
WHEN 'SHU1' THEN 'Huila'
WHEN 'SZH1' THEN 'Bajio'
WHEN 'SER1' THEN 'Interior'
WHEN 'SNQ1' THEN 'Interior'
WHEN 'STU1' THEN 'Interior'
WHEN 'SST1' THEN 'Interior'
WHEN 'SRD1' THEN 'Norte'
WHEN 'SBB2' THEN 'Sur'
WHEN 'SIC1' THEN 'Sur'
WHEN 'SOS1' THEN 'Sur'
WHEN 'SPO1' THEN 'Norte'
WHEN 'STM1' THEN 'Sur'
WHEN 'SSP32' THEN 'SP Capital'
ELSE shp_lg_facility_id 
END Region
,
CASE shp_lg_facility_id
WHEN 'SAM1' THEN 'Manaus'
WHEN 'SBA1' THEN 'Salvador'
WHEN 'SAL1' THEN 'Maceio'
WHEN 'SC_VG' THEN 'Vila Guilherme'
WHEN 'SSP2' THEN 'Vila Guilherme'
WHEN 'SC_ZS' THEN 'Zona Sul'
WHEN 'SCE1' THEN 'Fortaleza'
WHEN 'SDF1' THEN 'Brasilia'
WHEN 'SES1' THEN 'Vitoria'
WHEN 'SGO1' THEN 'Goiania'
WHEN 'SMG1' THEN 'Belo Horizonte'
WHEN 'SPE1' THEN 'Recife'
WHEN 'SPR1' THEN 'Curitiba'
WHEN 'SPR2' THEN 'Londrina'
WHEN 'SPR3' THEN 'Cascavel'
WHEN 'SPR4' THEN 'Pato Branco'
WHEN 'SRJ1' THEN 'Rio de Janeiro'
WHEN 'SRJ2' THEN 'Queimados'
WHEN 'SRJ3' THEN 'Volta Redonda'
WHEN 'SRJ4' THEN 'Campos dos Goytacazes'
WHEN 'SRJ5' THEN 'Petropolis'
WHEN 'SRS1' THEN 'Porto Alegre'
WHEN 'SSP10' THEN 'Araçatuba'
WHEN 'SSP11' THEN 'Presidente Prudente'
WHEN 'SSP12' THEN 'Sao Jose do Rio Preto'
WHEN 'SSP13' THEN 'Marolia'
WHEN 'SSP14' THEN 'Bauru'
WHEN 'SSP15' THEN 'Santos'
WHEN 'SSP16' THEN 'Caraguatatuba'
WHEN 'SSP17' THEN 'Sao Bernardo do Campo'
WHEN 'SSP3' THEN 'Campinas'
WHEN 'SSP4' THEN 'Ribeirio Preto'
WHEN 'SSP5' THEN 'Barueri'
WHEN 'SSP6' THEN 'Zona Leste'
WHEN 'SSP7' THEN 'Zona Oeste'
WHEN 'SSP8' THEN 'Sao Jose dos Campos'
WHEN 'SSP9' THEN 'Limeira'
WHEN 'SSC1' THEN 'Joinville'
WHEN 'SSC1' THEN 'Joinville'
WHEN 'SSC2' THEN 'Florianopolis'
WHEN 'SSC3' THEN 'Blumenau'
WHEN 'SSC4' THEN 'Chapeco'
WHEN 'SSP18' THEN 'Jundiao'
WHEN 'SMG2' THEN 'Juaz de Fora'
WHEN 'SSP19' THEN 'Guarulhos'
WHEN 'SSP20' THEN 'Sorocaba'
WHEN 'SSP21' THEN 'Mooca'
WHEN 'SSC2' THEN 'Florian�polis'
WHEN 'SMG3' THEN 'Pouso Alegre'
WHEN 'SMG4' THEN 'Ipatinga'
WHEN 'SMG5' THEN 'Poios de Caldas'
WHEN 'SMG6' THEN 'Uberlandia'
WHEN 'SSE1' THEN 'Aracajo'
WHEN 'SMG4' THEN 'Ipatinga'
WHEN 'SPR2' THEN 'Londrina'
WHEN 'SMR1' THEN 'Cuiabo'
WHEN 'SJP1' THEN 'Joao Pessoa'
WHEN 'SBU1' THEN 'Valentin Alsina'
WHEN 'SBU2' THEN 'Moron'
WHEN 'SBU3' THEN 'Munro'
WHEN 'SCF2' THEN 'Palacio'
WHEN 'SLA1' THEN 'La Plata'
WHEN 'SRO1' THEN 'Rosario'
WHEN 'SCF1' THEN 'CAD'
WHEN 'COCU01' THEN 'Bogota'
WHEN 'COXBG1' THEN 'Bogota'
WHEN 'SBO1' THEN 'Bogota'
WHEN 'SME1' THEN 'Mendoza'
WHEN 'SCO1' THEN 'Cordoba'
WHEN 'SAN1' THEN 'Medellin'
WHEN 'SRN1' THEN 'Natal'
WHEN 'SRS4' THEN 'Caxias do Sul'
WHEN 'SRJ10' THEN 'Barra'
WHEN 'SSP22' THEN 'Sao Carlos'
WHEN 'SMG8' THEN 'BH Zona Norte'
WHEN 'SMN1' THEN 'Sao Luis'
WHEN 'SPA1' THEN 'Belum'
WHEN 'SRJ9' THEN 'Cabo Frio'
WHEN 'SMG7' THEN 'Montes Claros'
WHEN 'SRS2' THEN 'Pelotas'
WHEN 'SPI1' THEN 'Teresina'
WHEN 'SRS3' THEN 'Santa Maria'
WHEN 'SBA3' THEN 'Vitoria da Conquista'
WHEN 'SRS5' THEN 'Passo Fundo'
WHEN 'SBA2' THEN 'Feira de Santana'
WHEN 'SRJ6' THEN 'Mendanha'
WHEN 'SRJ7' THEN 'Macao'
WHEN 'SRJ8' THEN 'Niteroi'
WHEN 'SVA1' THEN 'Cali'
WHEN 'SRS7' THEN 'Ijua'
WHEN 'SRI1' THEN 'Pereira'
WHEN 'SRS6' THEN 'Alegrete'
WHEN 'SPR6' THEN 'Maringa'
WHEN 'SMQ1' THEN 'Mar Del Plata'
WHEN 'SBY1' THEN 'Tunja'
WHEN 'SMS1' THEN 'Campo Grande'
WHEN 'SSP26' THEN 'Franca'
WHEN 'SSP29' THEN 'Mogi Mirim'
WHEN 'STO1' THEN 'Palmas'
WHEN 'SPR7' THEN 'Ponta Grossa'
WHEN 'SMG12' THEN 'Uberaba'
WHEN 'SSP27' THEN 'Itapetininga'
WHEN 'SGO2' THEN 'Rio Verde'
WHEN 'SSC5' THEN 'Criciuma'
WHEN 'SMS2' THEN 'Dourados'
WHEN 'SPR5' THEN 'Guarapuava'
WHEN 'SRO1' THEN 'Porto Velho'
WHEN 'SSP31' THEN 'Barretos'
WHEN 'SSP25' THEN 'Atibaia'
WHEN 'SMG9' THEN 'Varginha'
WHEN 'SRJ13' THEN 'Nova Friburgo'
WHEN 'SSC7' THEN 'Lages'
WHEN 'SMG13' THEN 'Teofilo Otoni'
WHEN 'SAP1' THEN 'Macapo'
WHEN 'SSC6' THEN 'Joaçaba'
WHEN 'SSP24' THEN 'Avaro'
WHEN 'SRS8' THEN 'Capao da Canoa'
WHEN 'SMG10' THEN 'Divinapolis'
WHEN 'SMG11' THEN 'Patos de Minas'
WHEN 'SRJ12' THEN 'Itaperuna'
WHEN 'SES2' THEN 'Cachoeiro do Itapemirim'
WHEN 'SBA4' THEN 'Itabuna'
WHEN 'SSP28' THEN 'Jales'
WHEN 'SRJ11' THEN 'Angra dos Reis'
WHEN 'SRS9' THEN 'Rio Grande'
WHEN 'SRS10' THEN 'Venincio Aires'
WHEN 'SSP30' THEN 'Registro'
WHEN 'SSP23' THEN 'Mogi das Cruzes'
WHEN 'SSA1' THEN 'Bucaramanga'
WHEN 'SSF1' THEN 'Santa Fe'
WHEN 'STI1' THEN 'Ibague'
WHEN 'SHU1' THEN 'Huila'
WHEN 'SER1' THEN 'Parana'
WHEN 'SNQ1' THEN 'Neuquen'
WHEN 'STU1' THEN 'Tucuman'
WHEN 'SST1' THEN 'Salta'
WHEN 'SRD1' THEN 'Porto Velho'
WHEN 'SSP32' THEN 'Franco da Rocha'
ELSE shp_lg_facility_id
END shp_lg_facility_name
from temp_45.lp_lastmile_MELI_step_01 lm
)WITH DATA;
-- DROPEO LAS TABLAS AUX
DROP TABLE TEMP_45.LP_LM_DETAILS_MELI;
DROP TABLE TEMP_45.LP_LM_TOTALS_MELI;
DROP TABLE temp_45.lp_lastmile_MELI_step_01;